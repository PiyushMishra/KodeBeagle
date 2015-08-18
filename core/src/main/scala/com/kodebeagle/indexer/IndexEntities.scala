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

import scala.collection.immutable

case class MethodToken(importName: String, importExactName: String, lineNumbers: List[HighLighter],
  methodAndLineNumbers: Set[MethodAndLines])

case class MethodAndLines(methodName: String, lineNumbers: List[HighLighter])

case class IndexEntry(repoId: Int, file: String, tokens: Set[Token], score: Int)

case class ImportsMethods(repoId: Int, file: String,
                          tokens: Set[MethodToken], score: Int)

/* Since our tokens are fully qualified import names. */
case class Token(importName: String, lineNumbers: immutable.Set[Int])

case class Token2(importName: String, importExactName: String,
    lineNumbers: immutable.Set[HighLighter])

case class HighLighter(lineNumber: Int, startColumn: Int, endColumn: Int)

case class SourceFile(repoId: Int, fileName: String, fileContent: String)

case class Repository(login: String, id: Int, name: String, fork: Boolean, language: String,
                      defaultBranch: String, stargazersCount: Int)

case class Statistics(repoId: Int, sloc: Int, fileCount: Int, size: Long)

case class Suggestion(imports: Set[Import], score: Int)

case class Import(importName: String, importExactName: String)

/** For testing */
object Repository {
  def invalid: Repository = Repository("n-a", -1, "n-a", fork = false, "Java", "n-a", 0)
}
