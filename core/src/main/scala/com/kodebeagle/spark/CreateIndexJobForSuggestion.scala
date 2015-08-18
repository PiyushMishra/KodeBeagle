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
import com.kodebeagle.indexer._
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object CreateIndexJobForSuggestion {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster).
      setAppName("CreateIndexJobForMethods").set("spark.executor.memory", "2g")

    val sc: SparkContext = createSparkContext(conf)

    val zipFileExtractedRDD: RDD[(List[(String, String)],
      Option[Repository], List[String], Statistics)] = makeZipFileExtractedRDD(sc)

    // Create indexes for elastic search.
    zipFileExtractedRDD.map { f =>
      val (files, repo, packages, stats) = f
      (repo, new JavaASTBasedIndexerForMethods().
        generateTokensWithMethods(files.toMap, packages, repo), mapToSourceFiles(repo, files),
        stats)
    }.flatMap { case (_, importsMethods, _, _) => 
      importsMethods.map(_.tokens.map(_.importExactName))
      case _ => Seq()
    }.groupBy(importSet => importSet).map {
      case (importSetKey, importSet) => importSetKey -> importSet.size
    }.filter { case (a, b) => a.size > 1}.map
      { case (importSetKey, importSet) => toJson(Suggestion(importSetKey.
        map(importName => Import(importName.toLowerCase(), importName)), importSet))}.
      saveAsTextFile(KodeBeagleConfig.sparkIndexOutput)
    }

}
