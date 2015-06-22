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

import java.io.PrintWriter

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.Repository
import com.kodebeagle.logging.Logger
import com.kodebeagle.parser.MethodVisitor
import com.kodebeagle.spark.CreateIndexJob.SourceFile
import com.kodebeagle.spark.HdfsZipFileExtracter
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.apache.commons.io.IOUtils
import org.apache.commons.compress.archivers.zip.ZipFile
import scala.collection.immutable

class JavaASTBasedIndexer extends BasicIndexer with Logger {
  import JavaFileIndexerHelper._

  val zipExtracter = new HdfsZipFileExtracter

  override def linesOfContext: Int = 0

  private def extractTokensASTParser(excludePackages: Set[String],
      fileContent: String, fileName: String): (Set[(String, String)], Set[Set[Token]]) = {
    val parser = new MethodVisitor()
    parser.parse(fileContent, fileName)
    import scala.collection.JavaConversions._
    val imports = parser.getImportDeclMap.toIterator.map(x => (x._2.stripSuffix(s".${x._1}"),
      x._1)).filterNot { case (left, right) => excludePackages.contains(left) }.toSet
    val importsSet = imports.map(tuple2ToImportString)

    (imports,
      parser.getListOflineNumbersMap.map(x => x.map(y => Token(y._1, y._2.map(_.toInt).toSet))
      .filter(x => importsSet.contains(x.importName)).toSet).toSet)
  }

  override def generateTokens(files: Map[String, String], excludePackages: List[String],
      repo: Option[Repository]): Set[IndexEntry] = {
    var indexEntries = immutable.HashSet[IndexEntry]()
    val r = repo.getOrElse(Repository.invalid)
    for (file <- files) {
      val (fileName, fileContent) = file
      val fullGithubURL = fileNameToURL(r, fileName)
      try {
        indexEntries = indexEntries ++ getTokens(r, fileContent, fileName,
          fullGithubURL, excludePackages)
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          log.error(s"Failed for $fullGithubURL" + e.getStackTrace.map(_.toString).mkString("\n"));
      }
    }
    indexEntries
  }

  def generateTokens(zipFile: ZipFile, repo: Option[Repository]): Unit = {
    createFolderIfNotExists(KodeBeagleConfig.sparkIndexOutput)
    import scala.collection.JavaConversions._
    import com.kodebeagle.crawler.ZipHelper._
    import scala.util.Try
    val bufferSize = 1024000
    val (allJavaFiles, excludePackages) = extractZipEntriesAndPackages(zipFile.getEntries.toList)
    var indexEntries = immutable.HashSet[IndexEntry]()
    val r = repo.getOrElse(Repository.invalid)
    import FileContentAppender._
    for (fileZipEntry <- allJavaFiles) {
      val b = new Array[Byte](bufferSize)
      val fileContent = Try(IOUtils.read(zipFile.getInputStream(fileZipEntry), b)).
        toOption.map(x => new String(b).trim).getOrElse("")
      val fileName = fileZipEntry.getName
      val fullGithubURL = fileNameToURL(r, fileName)
      try {
        indexEntries = indexEntries ++ getTokens(r, fileContent, fileName,
          fullGithubURL, excludePackages)
        appendToFile(KodeBeagleConfig.sparkIndexOutput + r.id,
          toJson(SourceFile(r.id, fileName, fileContent), isToken = false) + "\n")
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          log.error(s"Failed for $fullGithubURL", e);
      }
    }
    appendToFile(KodeBeagleConfig.sparkIndexOutput + r.id, toJson(r, isToken = false) + "\n" +
      toJson(indexEntries, isToken = true))
  }

  def generateTokens(hdfsFilePath: String, repo: Option[Repository]): Unit = {
    createFolderIfNotExists(KodeBeagleConfig.sparkIndexOutput)
    try {
      val (_, excludePackages) = zipExtracter.readFileNameAndPackages(hdfsFilePath)
      var indexEntries = immutable.HashSet[IndexEntry]()
      val r = repo.getOrElse(Repository.invalid)
      val stream = zipExtracter.makeZipFileStream(hdfsFilePath)
      import java.util.zip.ZipEntry
      var ze: Option[ZipEntry] = None
      import FileContentAppender._
      do {
        ze = Option(stream.getNextEntry)
        ze.map { ze =>
          if (ze.getName.endsWith("java") && !ze.isDirectory) {
            val fileName = ze.getName
            val fileContent = zipExtracter.readContent(stream, ze)
            val fullGithubURL = fileNameToURL(r, fileName)
            try {
              indexEntries = indexEntries ++ getTokens(r, fileContent, fileName,
                fullGithubURL, excludePackages)
              appendToFile(KodeBeagleConfig.sparkIndexOutput + r.id,
                toJson(SourceFile(r.id, fileName, fileContent), isToken = false) + "\n")
            } catch {
              case e: Throwable =>
                log.error(s"Failed for $fullGithubURL", e)
            }
          }
        }
      }
      while (ze != None)
      stream.close
      appendToFile(KodeBeagleConfig.sparkIndexOutput + r.id, toJson(r, isToken = false) + "\n" +
        toJson(indexEntries, isToken = true) + "\n")
    } catch {
      case ex: Exception => log.error(s"Failed for $repo",  ex)
    }
  }

  def getTokens(r: Repository, fileContent: String, fileName: String,
    fullGithubURL: String, excludePackages: List[String]): Set[IndexEntry] = {
    val (imports, tokens) = extractTokensASTParser(excludePackages.toSet, fileContent, fileName)
    val score =
      if (isTestFile(imports)) r.stargazersCount / penalizeTestFiles else r.stargazersCount
    tokens.map { y =>
      IndexEntry(r.id, fullGithubURL, y, score)
    }
  }

  def createFolderIfNotExists(folderName:String) = {
    val folder = new java.io.File(folderName)
    if(!folder.exists()) folder.mkdirs()
  }
}
