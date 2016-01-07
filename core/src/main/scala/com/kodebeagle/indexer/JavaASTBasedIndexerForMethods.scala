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


package com.kodebeagle.indexer

import com.kodebeagle.indexer.JavaFileIndexerHelper._
import com.kodebeagle.parser.MethodVisitor

import scala.collection.immutable

class JavaASTBasedIndexerForMethods extends JavaASTBasedIndexer {

  private def extractTokensWithTypesASTParser(excludePackages: Set[String],
    fileContent: String, fileName: String):
  (Set[(String, String)], List[Set[MethodToken]], Set[String]) = {
    import scala.collection.JavaConversions._
    val parser = new MethodVisitor()
    parser.parse(fileContent, fileName)
    import com.kodebeagle.parser.MethodVisitorHelper._
    val imports: Set[(String, String)] = getImports(parser,excludePackages)
    val importsSet = imports.map(tuple2ToImportString)
    val tokensMap: List[Map[String, List[HighLighter]]] = getTokenMap(parser, importsSet)
    (imports, createMethodIndexEntries(parser, tokensMap), parser.types.toSet)
  }

  def generateTokensWithMethods(files: Map[String, String], excludePackages: List[String],
                                repo: Option[Repository]):
  (Set[ImportsMethods], Map[String, Set[String]]) = {
    var indexEntries = immutable.HashSet[ImportsMethods]()
    var fileAndTypes: Map[String, Set[String]]= Map()
    val r = repo.getOrElse(Repository.invalid)
    for (file <- files) {
      val (fileName, fileContent) = file
      val fullGithubURL = fileNameToURL(r, fileName)
      try {
        val (imports, methodTokens, types) =
          extractTokensWithTypesASTParser(excludePackages.toSet, fileContent, fileName)
        fileAndTypes = fileAndTypes ++ Map(fullGithubURL -> types)
        val score =
          if (isTestFile(imports)) r.stargazersCount / penalizeTestFiles else r.stargazersCount
        for (methodToken <- methodTokens) {
          indexEntries =
            indexEntries + ImportsMethods(r.id, fullGithubURL, methodToken,
              score, JavaFileIndexerHelper.getLang(fullGithubURL))
        }
      } catch {
        case e: Throwable => log.error(s"Failed for $fullGithubURL", e);
      }
    }
    (indexEntries.filter(x => x.language.nonEmpty && x.tokens.nonEmpty), fileAndTypes)
  }
}
