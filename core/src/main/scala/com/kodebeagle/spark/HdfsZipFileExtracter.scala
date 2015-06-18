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

import java.io.ByteArrayOutputStream
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import com.kodebeagle.crawler.ZipHelper._
/**
 * Created by piyushm on 12/6/15.
 */
class HdfsZipFileExtracter {

  val conf = new Configuration()
  conf.addResource(new Path("/opt/software/hadoop-2.6.0/etc/hadoop/core-site.xml"))

  def makeZipFileStream(hdfsFilePath: String): ZipInputStream = {
    val fs = FileSystem.get(conf)
    val path = new Path(hdfsFilePath)
    val fin = fs.open(path)
    new ZipInputStream(fin)
  }

  def readZipFileHeader(stream: ZipInputStream): List[ZipEntry] = {
    var entries = List[ZipEntry]()
    var ze: Option[ZipEntry] = None
    do {
      ze = Option(stream.getNextEntry)
      if (ze != None) {
        entries = entries.+:(ze.get)
      }
    } while (ze != None)
    stream.close()
    entries
  }

  def readFileNameAndPackages(hdfsFilePath: String): (List[ZipEntry], List[String]) = {
    val stream = makeZipFileStream(hdfsFilePath)
    val zipArchiveEntries = readZipFileHeader(stream)
    extractZipEntriesAndPackages(zipArchiveEntries)
  }

  def readContent(stream: ZipInputStream, entry: ZipEntry): String = {
    val output = new ByteArrayOutputStream()
    var data: Int = 0
    do {
      data = stream.read()
      if (data != -1) output.write(data)
    } while (data != -1)
    val kmlBytes = output.toByteArray();
    output.close()
    new String(kmlBytes, "utf-8").trim
  }
}
