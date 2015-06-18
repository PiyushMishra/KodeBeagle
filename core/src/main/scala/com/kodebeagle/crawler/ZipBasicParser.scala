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

import java.io.File
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.io.IOUtils
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import com.kodebeagle.crawler.ZipHelper._

/**
 * Extracts java files and packages from the given zip file.
 */
object ZipBasicParser {

  private val bufferSize = 1024000 // about 1 mb

  def readFilesAndPackages(zip: ZipFile): (ArrayBuffer[(String, String)], List[String]) = {
    val list = mutable.ArrayBuffer[(String, String)]()
    val (allJavaFiles, allPackages) = extractZipEntriesAndPackages(zip.getEntries.toList)
    val files = allJavaFiles.map(x => x.getName -> zip.getInputStream(x))
    for ((name, f) <- files) {
      val b = new Array[Byte](bufferSize)
      Try(IOUtils.read(f, b)).toOption.foreach(x => list += (name -> new String(b).trim))
    }
    (list, allPackages)
  }

  def listAllFiles(dir: String): Array[File] = new File(dir).listFiles

}

object ZipHelper {

  private def fileNameToPackageName(s: String) = {
    val (_, packageN) = s.splitAt(s.indexOf("/src/"))
    packageN.stripPrefix("/").stripSuffix("/").replace('/', '.').stripPrefix("src.main.java.")
  }

  def mapToPackageName[T <: java.util.zip.ZipEntry]: (List[T] => List[String]) = (list: List[T]) =>
    list.filter(x => x.isDirectory && x.getName.toLowerCase.matches(".*src/main/java.*"))
      .map (f => fileNameToPackageName(f.getName))

  def extractZipEntriesAndPackages[T <: java.util.zip.ZipEntry]
  : (List[T] => (List[T], List[String])) = (zipArchiveEntries: List[T]) => {
    val allJavaFiles = zipArchiveEntries.filter(x => x.getName.endsWith("java") && !x.isDirectory)
    val allPackages = mapToPackageName(zipArchiveEntries)
    (allJavaFiles, allPackages)
  }

}



