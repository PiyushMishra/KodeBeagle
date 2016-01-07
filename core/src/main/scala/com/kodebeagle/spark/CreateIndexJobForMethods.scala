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

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.parser._
import com.kodebeagle.indexer.{JavaASTBasedIndexerForMethods, Repository, Statistics}
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateIndexJobForMethods {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("CreateIndexJobForMethods")
      .set("spark.executor.memory", "2g")
      .set("spark.network.timeout", "1200s")

    val sc: SparkContext = createSparkContext(conf)

    val zipFileExtractedRDD: RDD[(List[(String, String)], List[(String, String)],
      Option[Repository], List[String], Statistics)] = makeZipFileExtractedRDD(sc)

    // Create indexes for elastic search.

    zipFileExtractedRDD.map { f =>
      val (javaFiles, scalaFiles, repo, packages, stats) = f
      val (tokens, javaTypes) = new JavaASTBasedIndexerForMethods()
        .generateTokensWithMethods(javaFiles.toMap, packages, repo)
      (repo, tokens, ScalaParser.generateTokensWithBraceContext(scalaFiles.toMap, packages, repo),
        mapToSourceFiles(repo, javaFiles ++ scalaFiles, javaTypes), stats)
    }.flatMap { case (Some(repository), javaIndices, scalaIndices,
    sourceFiles, stats) => Seq(toJson(repository, isToken = false),
      toJson(javaIndices,isToken = false), toJson(scalaIndices, isToken = false),
      toJson(sourceFiles, isToken = false),
        toJson(stats, isToken = false))
    case _ => Seq()
    }.saveAsTextFile(KodeBeagleConfig.sparkIndexOutput)
  }
}
