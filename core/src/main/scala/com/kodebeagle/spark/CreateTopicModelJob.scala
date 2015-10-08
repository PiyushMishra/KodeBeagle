/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kodebeagle.spark

import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import com.kodebeagle.configuration.KodeBeagleConfig
import org.apache.spark.SparkContext
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.elasticsearch.spark._
import org.apache.spark.HashPartitioner
import codesum.lm.main.ASTVisitors.TreeCreatorVisitor
import codemining.java.codeutils.JavaASTExtractor
import codemining.languagetools.ParseType
import org.eclipse.jdt.core.dom.CompilationUnit
import java.util.ArrayList
import codesum.lm.main.Settings
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.TripletFields
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.VertexRDD
import scala.util.Random
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.broadcast.Broadcast
import com.kodebeagle.ml.LDA
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.graphx.impl.EdgeRDDImpl
import scala.util.Try
import com.kodebeagle.logging.Logger
import com.kodebeagle.configuration.TopicModelConfig
import com.kodebeagle.ml.LDAModel
import com.kodebeagle.ml.DistributedLDAModel

object CreateTopicModelJob extends Logger {

  case class Node(name: String, parentId: Long, id: Long = -22) extends Serializable

  val jobName = TopicModelConfig.jobName
  val nbgTopics = TopicModelConfig.nbgTopics
  val nIterations = TopicModelConfig.nIterations
  val nWordsDesc = TopicModelConfig.nDescriptionWords
  val chkptInterval = TopicModelConfig.chkptInterval
  val batchSize = TopicModelConfig.batchSize
  val esPortKey = "es.port"
  val esNodesKey = "es.nodes"
  val topicFieldName = "topic"
  val termFieldName = "term"
  val freqFieldName = "freq"
  val fileFieldName = "file"
  val klScoreFieldName = "klscore"
  val filesFieldName = "files"


  def main(args: Array[String]): Unit = {
    var repoCounter = 0
    var fetched = 0
    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster).setAppName(jobName)
    conf.set(esNodesKey, KodeBeagleConfig.esNodes)
    conf.set(esPortKey, KodeBeagleConfig.esPort)
    var sc: SparkContext = createSparkContext(conf)
    sc.setCheckpointDir(KodeBeagleConfig.sparkCheckpointDir)

    val repos = sc.esRDD(KodeBeagleConfig.esRepositoryIndex)
    // TODO: How to do this without materializing upfront.
    val repoIds = repos.map({
      case ((recordId, valueMap)) =>
        (recordId, valueMap.get("id").get.toString())
    }).collect()

    while ((repoCounter == 0 || fetched == batchSize) && repoCounter >= 0) {
      sc.stop()
      sc = createSparkContext(conf)
      sc.setCheckpointDir(KodeBeagleConfig.sparkCheckpointDir)
      val currBatch = repoIds.slice(repoCounter, repoCounter + batchSize)
      fetched = currBatch.length
      repoCounter += fetched
      if (fetched > 0) runOnRepos(sc, currBatch)
    }

  }

  /**
   * Helper functions
   */
  private def fetchReposFromEs(sc: SparkContext,
    repoIds: Array[(String, String)]): RDD[(Int, (String, String))] = {
    var ids = repoIds.map(_._2).mkString(",")
    val query = s"""{"query":{"terms": {"repoId": [${ids}]}}}"""

    val repoSources = sc.esRDD(KodeBeagleConfig.esourceFileIndex, Map("es.query" -> query))
      .map({
        case (repoId, valuesMap) => {
          (valuesMap.get("repoId").getOrElse(0).asInstanceOf[Int],
            (valuesMap.get("fileName").getOrElse("").asInstanceOf[String],
              valuesMap.get("fileContent").getOrElse("").toString))
        }
      })
    log.info(s"Querying files for repos: $query , files fetched: ${repoSources.count()}")
    repoSources
  }

  private def runOnRepos(sc: SparkContext, repoIds: Array[(String, String)]) = {
    val offset = nbgTopics
    val repoSources = fetchReposFromEs(sc, repoIds)
    val repoIdVsDocIdMap = repoIds.map({
      case (recordId, repoId) =>
        (repoId, recordId)
    }).toMap
    val repoIdVsFiles = repoSources.groupBy(_._1)
    val repos = repoIdVsFiles.map(f => new Node(f._1.toString(), 0))
    val (repoNameVsId, repoIdVsName, repoVertices) = extractVocab(repos, offset)
    val fileIdOffset = repoNameVsId.size + offset
    val files = repoSources.map({
      case (repoId, (fileName, fileContent)) =>
        new Node(repoId + ":" + fileName, repoNameVsId.get(repoId.toString()).get)
    }).distinct()
    val (fileNameVsId, fileIdVsName, fileVertices) = extractVocab(files, fileIdOffset);

    val words = repoSources.map({
      case (repoId, (fileName, fileContent)) =>
        (fileName, (repoId, fileContent))
    }).repartition(sc.defaultParallelism)
      .flatMap({
        case (fileName, (repoId, fileContent)) =>
          val tokenListTry = Try(getTokensForFile(fileContent))
          var tokenList: java.util.List[String] = new java.util.ArrayList[String]()
          if (tokenListTry.isSuccess) {
            tokenList = tokenListTry.get
          }
          val tokens = tokenList.map { t =>
            new Node(t, fileNameVsId.get(repoId.toString() + ":" + fileName).get)
          }
          tokens
      })
    val wordIdOffset = fileNameVsId.size + repoNameVsId.size + offset
    val wordVocab = words.map(f => (f.name, "")).aggregateByKey(0)((x, y) => 0, (l, r) => 0)
      .keys.zipWithIndex().map({
        case (word, wordId) =>
          (word, wordId + wordIdOffset)
      }).collect().toMap
    val tokenToWordMap = wordVocab.map({ case (word, wordId) => (wordId, word) }).toMap

    val edges = words.map(f => (wordVocab.get(f.name).get, f.parentId)).cache()
    val docGroupings = fileVertices.map({
      case ((fileId, fileNode)) =>
        (fileId, fileNode.parentId)
    }).cache()
    val result = new LDA().setMaxIterations(nIterations)
      .setK(nbgTopics).setCheckPointInterval(chkptInterval)
      .runFromEdges(edges, Option(docGroupings));
    handleResult(result, sc, repoIdVsName, tokenToWordMap, repoIdVsDocIdMap, fileIdVsName)
  }

  def handleResult(result: DistributedLDAModel,
    sc: SparkContext,
    repoIdVsName: mutable.Map[Long, String],
    tokenToWordMap: Map[Long, String],
    repoIdVsDocIdMap: Map[String, String],
    fileIdVsName: mutable.Map[Long, String]): Unit = {

    val topics = result.describeTopics(nWordsDesc)
    logTopics(topics, repoIdVsName, tokenToWordMap)

    var i = nbgTopics
    val repoTopics = topics.slice(nbgTopics, topics.length)
    // For each repo, map of topic terms vs their frequencies 
    val repoTopicFields = for (topic <- repoTopics) yield {
      val repoName = repoIdVsName.get(i.toLong).get
      val topicMap = topic.map({ case (count, wordId) =>
        (tokenToWordMap(wordId.toLong), count)
      }).toMap
      i += 1
      (repoName, topicMap)
    }

    val repoSummary = result.summarizeDocGroups()
    logRepoSummary(repoSummary, repoIdVsName, fileIdVsName)

    val repoSummaryRdd = sc.makeRDD(repoSummary)
    // For each repo, map of files vs their score.
    val repoFilescore = repoSummaryRdd.map({ case (repoId, fileId, klScore) =>
      val repoName = repoIdVsName.get(repoId).get
      val file = fileIdVsName.get((fileId * (-1L) - 1L)).get.split(":")(1)
      (repoName, file, klScore)
    }).groupBy(f => f._1).map(f => (f._1, f._2.map({ case (repoId, file, klscore) => (file, klscore) }).toMap))

    val updatedRepoRDD = sc.makeRDD(repoTopicFields).join(repoFilescore).map({ case (repoId, (repoTopicMap, repoFileMap)) =>
      val esTopicMap = repoTopicMap.map(f => Map(termFieldName -> f._1, freqFieldName -> f._2))
      val esFilesMap = repoFileMap.map(f => Map(fileFieldName -> f._1, klScoreFieldName -> f._2))
      val termMap = Map(topicFieldName -> esTopicMap)
      val topicMap = Map(filesFieldName -> esFilesMap)
      Map("_id" -> repoId) ++ termMap ++ topicMap
    })

    import SparkIndexJobHelper._

    if (TopicModelConfig.saveToLocal != "") {
      log.info(s"Saving to ${TopicModelConfig.saveToLocal}")
      updatedRepoRDD.map(repoRecord => toJson(TopicModel(repoRecord))).
        saveAsTextFile(TopicModelConfig.saveToLocal + java.util.UUID.randomUUID())
    }
    else {
      log.info(s"Saving to ${TopicModelConfig.saveToLocal}")
      updatedRepoRDD.saveToEs(KodeBeagleConfig.esRepoTopicIndex,
        Map("es.write.operation" -> "upsert", "es.mapping.id" -> "_id"))
    }
  }

  def logTopics(topics: Array[Array[(Int, Long)]],
    repoIdVsName: mutable.Map[Long, String],
    tokenToWordMap: Map[Long, String]): Unit = {
    val minFreq = 0
    for (i <- 0 until topics.size) {
      var topicName = "Background Topic "
      if (i < nbgTopics) {
        topicName = topicName + i
      } else {
        topicName = repoIdVsName.get(i.toLong).get
      }
      log.info(s"Topic Description for topic : $topicName")
      val sortedCounts = topics(i).sortBy(f => f._1).reverse
      sortedCounts.filter(x => x._1 > minFreq).foreach(z =>
        log.info(s"${z._1} : ${tokenToWordMap(z._2.toLong)}"))
    }
  }

  def logRepoSummary(repoSummary: Array[(Long, Long, Double)],
    repoIdVsName: mutable.Map[Long, String],
    fileIdVsName: mutable.Map[Long, String]): Unit = {
    repoSummary.groupBy(_._1).foreach(f => {
      log.info(s"Top docs for repo : ${repoIdVsName.get(f._1).get}")
      f._2.sortBy(_._3).filter(f =>
        !(fileIdVsName.get((f._2 * (-1L) - 1L)).get.contains("test"))).take(20)
        .foreach(f =>
          log.info(s"${f._3} - ${fileIdVsName.get((f._2 * (-1L) - 1L)).get.split(":")(1)}"))
    })

  }

  def getTokensForFile(fileContent: String): java.util.List[String] = {
    val tcv = new TreeCreatorVisitor();
    val ext = new JavaASTExtractor(false, true);
    val node = ext.getAST(fileContent, ParseType.COMPILATION_UNIT).asInstanceOf[CompilationUnit];
    tcv.process(node, fileContent, new Settings);

    val tokenList = new ArrayList[String]();
    val nodeCount = tcv.getTree().getNodeCount()

    var nodeID = 0;
    // Save foldable node tokens ordered by nodeID
    for (nodeID <- 0 until nodeCount) {
      for (s <- tcv.getIDTokens().get(nodeID)) {
        tokenList.add(s);
      }

    }
    tokenList;
  }

  def extractVocab(tokens: RDD[Node],
    startIndex: Int): (mutable.Map[String, Long], mutable.Map[Long, String], RDD[(Long, Node)]) = {
    val vocab = tokens.map(x => x.name).distinct().collect()
    var vocabLookup = mutable.Map[String, Long]()
    for (i <- 0 to (vocab.length - 1)) {
      vocabLookup += (vocab(i) -> (startIndex + i))
    }
    val nodesWithIds = tokens.map { x =>
      val id = vocabLookup.get(x.name).get
      val node = new Node(x.name, x.parentId, id)
      (id, node)
    }
    (vocabLookup, vocabLookup.map({ case (name, id) => (id, name) }), nodesWithIds)
  }

}

case class TopicModel(topicModel: Map[String, Object])