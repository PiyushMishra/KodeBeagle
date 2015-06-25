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

package com.kodebeagle.crawler

import java.io.{ByteArrayOutputStream, File}
import java.util.zip.{ZipEntry, ZipInputStream}

import com.kodebeagle.indexer.Statistics
import com.kodebeagle.logging.Logger
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * Extracts java files and packages from the given zip file.
 */
object ZipBasicParser extends Logger {

  private val bufferSize = 1024000 // about 1 mb

  private def fileNameToPackageName(s: String) = {
    val (_, packageN) = s.splitAt(s.indexOf("/src/"))
    packageN.stripPrefix("/").stripSuffix("/").replace('/', '.').stripPrefix("src.main.java.")
  }

  def readFilesAndPackages(repoId: Int, zipStream: ZipInputStream): (List[(String, String)],
    List[String], Statistics) = {
    val list = mutable.ArrayBuffer[(String, String)]()
    val allPackages = mutable.ArrayBuffer[String]()
    var size: Long = 0
    var fileCount: Int = 0
    var sloc: Int = 0
    var ze: Option[ZipEntry] = None
    try {
      do {
        ze = Option(zipStream.getNextEntry)
        ze.foreach { ze => if (ze.getName.endsWith("java") && !ze.isDirectory) {
          val fileName = ze.getName
          val fileContent = readContent(zipStream)
          size += fileContent.length
          list += (fileName -> fileContent)
          fileCount += 1
          sloc += fileContent.split("\n").size
        } else if (ze.isDirectory && ze.getName.toLowerCase.matches(".*src/main/java.*")) {
          allPackages += fileNameToPackageName(ze.getName)
        }
        }
      } while (ze.isDefined)
    } catch {
      case ex: Exception => log.error("Exception reading next entry {}", ex)
    }
    (list.toList, allPackages.toList, Statistics(repoId, sloc, fileCount, size / 1024))
  }

  def readContent(stream: ZipInputStream): String = {
    val output = new ByteArrayOutputStream()
    var data: Int = 0
    do {
      data = stream.read()
      if (data != -1) output.write(data)
    } while (data != -1)
    val kmlBytes = output.toByteArray
    output.close()
    new String(kmlBytes, "utf-8").trim
  }

  def listAllFiles(dir: String): Array[File] = new File(dir).listFiles
}

