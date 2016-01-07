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

import java.util.zip.ZipInputStream

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.ZipBasicParser
import com.kodebeagle.indexer.{Statistics, SourceFile, Repository}
import com.kodebeagle.parser.RepoFileNameParser

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SparkIndexJobHelper {

  def mapToSourceFiles(repo: Option[Repository],
    map: List[(String, String)], fileAndTypesMap: Map[String, Set[String]]):
  Set[SourceFile] = {
    val repo2 = repo.getOrElse(Repository.invalid)
    import com.kodebeagle.indexer.JavaFileIndexerHelper._
    map.map(x => SourceFile
    (repo2.id, fileNameToURL(repo2, x._1), x._2,
      fileAndTypesMap.getOrElse(fileNameToURL(repo2, x._1), Set()))).toSet
  }

  def createSparkContext(conf: SparkConf): SparkContext = new SparkContext(conf)

  def makeZipFileExtractedRDD(sc: SparkContext): RDD[(List[(String, String)],
    List[(String, String)], Option[Repository], List[String], Statistics)] = {
    val zipFileNameRDD = sc.binaryFiles(KodeBeagleConfig.githubDir).map {
      case (zipFile, stream) =>
      (zipFile.stripPrefix("file:").stripPrefix("hdfs:"), stream)
    }
    val zipFileExtractedRDD = zipFileNameRDD.map { case (zipFileName, stream) =>
      val repo: Option[Repository] = RepoFileNameParser(zipFileName)
      val (javaFilesMap, scalaFilesMap, packages, stats) =
        ZipBasicParser.readFilesAndPackages(
          repo.getOrElse(Repository.invalid).id, new ZipInputStream(stream.open()))
      (javaFilesMap, scalaFilesMap, repo, packages, stats)
    }
    zipFileExtractedRDD
  }

  /**
   * This currently uses star counts for a repo as a score.
   */
  def getGitScore(f: String): Option[Int] = {
    Try(f.stripSuffix(".zip").split("~").last.toInt).toOption
  }

  def getOrgsName(f: String): Option[String] = {
    Try(f.stripSuffix(".zip").split("~").tail.head).toOption
  }

  def toJson[T <: AnyRef <% Product with Serializable](t: Set[T], isToken: Boolean): String = {
    (for (item <- t) yield toJson(item, addESHeader = true, isToken = isToken)).mkString("\n")
  }

  def toJson[T <: AnyRef <% Product with Serializable](t: T, addESHeader: Boolean = true,
                                                       isToken: Boolean = false): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats = Serialization.formats(NoTypeHints)
    val indexName = t.productPrefix.toLowerCase
    if (addESHeader && isToken) {
      """|{ "index" : { "_index" : "kodebeagle", "_type" : "custom" } }
        | """.stripMargin + write(t)
    } else if (addESHeader) {
      s"""|{ "index" : { "_index" : "$indexName", "_type" : "type$indexName" } }
         |""".stripMargin + write(t)
    } else "" + write(t)

  }
}
