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

import com.kodebeagle.configuration.{KodeBeagleConfig, TopicModelConfig}
import com.kodebeagle.indexer. SourceFile
import com.kodebeagle.logging.Logger
import com.kodebeagle.parser.MethodVisitor
import com.kodebeagle.spark.SparkIndexJobHelper.createSparkContext
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._


object CreateFileTypesIndexJob extends Logger {


  val jobName = TopicModelConfig.jobName
  val esPortKey = "es.port"
  val esNodesKey = "es.nodes"

  import scala.collection.JavaConversions._

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster).setAppName(jobName)
    conf.set(esNodesKey, KodeBeagleConfig.esNodes)
    conf.set(esPortKey, KodeBeagleConfig.esPort)
    val sc: SparkContext = createSparkContext(conf)

    val repos = sc.esRDD("sourcefile/typesourcefile")

    val repoSources = sc.esRDD(KodeBeagleConfig.esourceFileIndex).map { case (repoId, valuesMap) =>
      SourceFile(valuesMap.get("repoId").getOrElse(0).asInstanceOf[Int],
        valuesMap.get("fileName").getOrElse("").asInstanceOf[String],
        valuesMap.get("fileContent").getOrElse("").toString, Set())
    }

    val m = repoSources.mapPartitions { souces => souces.map { so =>
      val parser  = new MethodVisitor()
      parser.parse(so.fileContent, so.fileName)
      SourceFile(so.repoId,so.fileName, so.fileContent, parser.types.toSet)
    }
    }
    import SparkIndexJobHelper._
    m.persist(MEMORY_AND_DISK)
    m.filter {case so => !so.types.isEmpty}.
      map {case k => toJson(k)}.
      saveAsTextFile(KodeBeagleConfig.sparkIndexOutput)
    m.filter {case so => (so.types.isEmpty)} map {
      case k =>
      ("did not get types info for file " + k.fileName)} saveAsTextFile(KodeBeagleConfig.sparkIndexOutput +
      "/demofailed")

  }
}
