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

import java.io.StringWriter

import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class JavaFileIndexerSuite extends FunSuite with BeforeAndAfterAll {
  val stream =
    Thread.currentThread().getContextClassLoader.getResourceAsStream("TransportClient.java")
  val writer = new StringWriter()
  val allOccurrences = List(74, 75, 78, 79, 101, 103, 105, 106, 108, 109, 114, 117, 119, 120, 121,
    123, 125, 137, 139, 141, 144, 145, 150, 154, 156, 158, 160, 172, 187, 188, 189, 191, 198, 203,
    204)
  val sampleRepo = Repository("sample", 0, "sample", false, "Java", "master", 0)

  override def beforeAll() {
    IOUtils.copy(stream, writer)
  }

  test("Parse a file and verify tokens when lines of context is more than file size") {
    val javaFileIndexer = new JavaFileIndexer {
      override val linesOfContext = 2000
    }
    val resultTokens = javaFileIndexer.generateTokens(Map("sample-master/Sample.java" -> writer
      .toString), List(), Some(Repository.invalid))
    assert(resultTokens.size === 1)
    val result = resultTokens.flatMap(_.tokens.flatMap(_.lineNumbers)).toList.distinct.sorted
    assert(result === allOccurrences)
  }

  test("Parse a file and verify tokens when lines of context is much less than file size") {
    val javaFileIndexer = new JavaFileIndexer {
      override val linesOfContext = 10
    }
    val result = javaFileIndexer.generateTokens(
      Map("sample-master/Sample.java" -> writer.toString), List(), Some(Repository.invalid))
    val occurrences = result.flatMap(x => x.tokens.map(_.lineNumbers)).reduce(_ ++ _)
    assert(occurrences.toList === allOccurrences)
    assert(result.size == 60)
  }

  test("Excluded imports should not be part of Tokens") {
    val javaFileIndexer = new JavaFileIndexer {
      override val linesOfContext = 20
    }
    val excludes = Set("org.apache.spark", "org.apache", "org",
      "org.apache.spark.network.protocol")
    val resultWOExcludes = javaFileIndexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), List(), Some(Repository.invalid))
    val resultWExcludes = javaFileIndexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), excludes.toList, Some(Repository.invalid))
    val expected = resultWOExcludes.flatMap(x => x.tokens.map(_.importName))
      .filterNot(_.startsWith("org.apache.spark.network.protocol"))
    val result = resultWExcludes.flatMap(x => x.tokens.map(_.importName))
    assert(result === expected)
  }

  test("Should not include global variables and declarations in indexes.") {
    val javaFileIndexer = new JavaFileIndexer {
      override val linesOfContext = 2000
    }
    val resultTokens = javaFileIndexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), List(), Some(Repository.invalid))
    val result = resultTokens.flatMap(_.tokens.flatMap(_.lineNumbers)).toList.distinct.sorted
    assert(!result.exists(Set(67, 68, 70)))
  }

  test("Should exclude commented code for processing indexes.") {
    val javaFileIndexer = new JavaFileIndexer {
      override val linesOfContext = 2000
    }
    val resultTokens = javaFileIndexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), List(), Some(Repository.invalid))
    val result = resultTokens.flatMap(_.tokens.flatMap(_.lineNumbers)).toList.distinct.sorted
    assert(!result.exists(Set(77, 82)))
  }

}

class JavaASTBasedIndexerSuite extends FunSuite with BeforeAndAfterAll {
  val stream =
    Thread.currentThread().getContextClassLoader.getResourceAsStream("TransportClient.java")
  val writer = new StringWriter()
  val allOccurrences = List(74, 75, 78, 79, 101, 103, 105, 106, 108, 109, 112, 114, 117, 118, 119,
    120, 121, 123, 125, 137, 139, 141, 144, 145, 148, 150, 153, 154, 156, 158, 160, 172, 177, 182,
    187, 188, 189, 191, 198, 203, 204)
  val sampleRepo = Repository("sample", 0, "sample", false, "Java", "master", 0)

  override def beforeAll() {
    IOUtils.copy(stream, writer)
  }

  test("Parse a file and verify tokens") {
    val indexer = new JavaASTBasedIndexer
    val resultTokens = indexer.generateTokens(Map("sample-master/Sample.java" -> writer
      .toString), List(), Some(Repository.invalid))
    assert(resultTokens.size === 7)
    val result = resultTokens.flatMap(_.tokens.flatMap(_.lineNumbers)).toList.distinct.sorted
    assert(result === allOccurrences)
  }

  test("Excluded imports should not be part of Tokens") {
    val indexer = new JavaASTBasedIndexer
    val excludes = Set("org.apache.spark", "org.apache", "org",
      "org.apache.spark.network.protocol")
    val resultWOExcludes = indexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), List(), Some(Repository.invalid))
    val resultWExcludes = indexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), excludes.toList, Some(Repository.invalid))
    val expected = resultWOExcludes.flatMap(x => x.tokens.map(_.importName))
      .filterNot(_.startsWith("org.apache.spark.network.protocol"))
    val result = resultWExcludes.flatMap(x => x.tokens.map(_.importName))
    assert(expected === result)
  }

  test("Should not include global variables and declarations in indexes.") {
    val indexer = new JavaASTBasedIndexer
    val resultTokens = indexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), List(), Some(Repository.invalid))
    val result = resultTokens.flatMap(_.tokens.flatMap(_.lineNumbers)).toList.distinct.sorted
    assert(!result.exists(Set(67, 68, 70)))
  }

  test("Should exclude commented code for processing indexes.") {
    val indexer = new JavaASTBasedIndexer
    val resultTokens = indexer.generateTokens(Map("sample-master/Sample.java" ->
      writer.toString), List(), Some(Repository.invalid))
    val result = resultTokens.flatMap(_.tokens.flatMap(_.lineNumbers)).toList.distinct.sorted
    assert(!result.exists(Set(77, 82)))
  }

}

class JavaASTBasedIndexerForMethodsSuite extends FunSuite with BeforeAndAfterAll {
  val stream =
    Thread.currentThread().getContextClassLoader.getResourceAsStream("TransportClient.java")
  val writer = new StringWriter()
  val sampleRepo = Repository("sample", 0, "sample", false, "Java", "master", 0)

  override def beforeAll() {
    IOUtils.copy(stream, writer)
  }

  test("Parse a file and verify method tokens") {
    val indexer = new JavaASTBasedIndexerForMethods
    val methodTokens = indexer.generateTokensWithMethods(Map("sample-master/Sample.java" -> writer
      .toString), List(), Some(Repository.invalid))

    assert(methodTokens.size === 7)
  
    val importChannel: String = "io.netty.channel.Channel"
    val importObjects: String = "com.google.common.base.Objects"
    val importLogger: String = "org.slf4j.Logger"
    val importNettyUtils: String = "org.apache.spark.network.util.NettyUtils"
    val importIOE: String = "java.io.IOException"
    val importUUID: String = "java.util.UUID"
    val importTimeUnit: String = "java.util.concurrent.TimeUnit"
    val importRpcReq: String = "org.apache.spark.network.protocol.RpcRequest"
    val importEE: String = "java.util.concurrent.ExecutionException"
    val importThrowables: String = "com.google.common.base.Throwables"
    val importSFuture: String = "com.google.common.util.concurrent.SettableFuture"
    val importPrecon: String = "com.google.common.base.Preconditions"
    val importSCId: String = "org.apache.spark.network.protocol.StreamChunkId"
    val importCFReq: String = "org.apache.spark.network.protocol.ChunkFetchRequest"
    val importXMLAnnotation: String = "javax.xml.bind.annotation.XmlAnyAttribute"
    val sampleFile: String = "n-a/n-a/blob/n-a/Sample.java"

    val testMethodTokens = Set(ImportsMethods(-1,sampleFile,
      Set(MethodToken(importChannel.toLowerCase, importChannel,
      List(HighLighter(204,28,34)), Set(MethodAndLines("remoteAddress",
          List(HighLighter(204,28,50))))),
        MethodToken(importObjects.toLowerCase,importObjects, List(HighLighter(203,12,18))
          ,Set(MethodAndLines("toStringHelper",List(HighLighter(203,12,39)))))),0),

      ImportsMethods(-1,sampleFile,Set(MethodToken(importTimeUnit.toLowerCase,
        importTimeUnit, List(HighLighter(187,36,43)),Set()),
        MethodToken(importEE.toLowerCase,importEE,List(HighLighter(188,7,5), HighLighter(189,34,34))
          , Set(MethodAndLines("getCause",List(HighLighter(189,34,45))))),
        MethodToken(importThrowables.toLowerCase,importThrowables,List(HighLighter(189,13,22),
          HighLighter(191,13,22)),Set(MethodAndLines("propagate",List(HighLighter(189,13,46),
          HighLighter(191,13,35))))), MethodToken(importSFuture.toLowerCase,
          importSFuture,List(HighLighter(182,9,14), HighLighter(187,14,19), HighLighter(172,11,32),
            HighLighter(172,43,56), HighLighter(177,9,14)),Set(MethodAndLines("set",
            List(HighLighter(177,9,28))), MethodAndLines("get",List(HighLighter(187,14,57))),
            MethodAndLines("create",List(HighLighter(172,43,65))),
            MethodAndLines("setException",List(HighLighter(182,9,30)))))),0),

      ImportsMethods(-1,sampleFile,Set(MethodToken(importTimeUnit.toLowerCase,
        importTimeUnit,List(HighLighter(198,46,53)),Set()),
        MethodToken(importChannel.toLowerCase,importChannel,List(HighLighter(198,5,11)),
          Set(MethodAndLines("close", List(HighLighter(198,5,19)))))),0),

      ImportsMethods(-1,sampleFile,Set(MethodToken(importXMLAnnotation.toLowerCase,
        importXMLAnnotation, List(HighLighter(78,4,18)), Set()),
        MethodToken("io.netty.channel.channel", "io.netty.channel.Channel",
          List(HighLighter(79,12,18), HighLighter(79,32,38)),
        Set(MethodAndLines("isOpen",List(HighLighter(79,12,27))),
          MethodAndLines("isActive",List(HighLighter(79,32,49)))))),0),

      ImportsMethods(-1,sampleFile,Set(MethodToken(importChannel.toLowerCase(),
        importChannel, List(HighLighter(74,10,16), HighLighter(74,47,53)),Set()),
        MethodToken(importPrecon.toLowerCase, importPrecon,List(HighLighter(74,20,32),
          HighLighter(75,20,32)),Set(MethodAndLines("checkNotNull",List(HighLighter(74,20,54),
          HighLighter(75,20,54)))))),0),

      ImportsMethods(-1,sampleFile,
        Set(MethodToken(importCFReq.toLowerCase(),
          importCFReq,List(HighLighter(108,31,47)),Set()),
          MethodToken(importChannel.toLowerCase,importChannel,List(HighLighter(101,59,65),
            HighLighter(121,13,19), HighLighter(108,5,11)),
            Set(MethodAndLines("close",List(HighLighter(121,13,27))),
              MethodAndLines("writeAndFlush",List(HighLighter(108,5,63))))),
          MethodToken(importLogger.toLowerCase,importLogger,List(HighLighter(103,5,10),
            HighLighter(114,13,18), HighLighter(119,13,18), HighLighter(125,15,20)),
            Set(MethodAndLines("trace",List(HighLighter(114,13,24))),
              MethodAndLines("debug",List(HighLighter(103,5,80))),
              MethodAndLines("error",List(HighLighter(119,13,50), HighLighter(125,15,85))))),
          MethodToken(importSCId.toLowerCase,
            importSCId,List(HighLighter(120,40,52), HighLighter(117,84,96), HighLighter(106,29,41),
              HighLighter(105,11,23), HighLighter(108,49,61), HighLighter(114,65,77),
              HighLighter(105,45,57)),Set()),
          MethodToken(importChannel.toLowerCase + "futurelistener",
            importChannel + "FutureListener", List(HighLighter(109,11,31)),Set()),
          MethodToken(importNettyUtils.toLowerCase, importNettyUtils,
            List(HighLighter(101,31,40)),Set(MethodAndLines("getRemoteAddress",
              List(HighLighter(101,31,66))))),
          MethodToken(importIOE.toLowerCase,importIOE,List(HighLighter(123,50,60)),Set()),
          MethodToken(importChannel.toLowerCase + "future",importChannel + "Future",
            List(HighLighter(112,15,20), HighLighter(118,27,32), HighLighter(119,36,41),
              HighLighter(123,72,77)), Set(MethodAndLines("cause",List(HighLighter(118,27,40),
              HighLighter(119,36,49), HighLighter(123,72,85))), MethodAndLines("isSuccess",
              List(HighLighter(112,15,32)))))),0),

      ImportsMethods(-1,sampleFile,
        Set(MethodToken(importUUID.toLowerCase,importUUID,List(HighLighter(141,37,40)),
          Set(MethodAndLines("randomUUID",List(HighLighter(141,37,53))))),
          MethodToken(importIOE.toLowerCase, importIOE,List(HighLighter(158,38,48)),Set()),
          MethodToken(importChannel.toLowerCase,importChannel,List(HighLighter(137,59,65),
            HighLighter(156,13,19), HighLighter(144,5,11)),Set(MethodAndLines("close",
            List(HighLighter(156,13,27))), MethodAndLines("writeAndFlush",
            List(HighLighter(144,5,61))))),
          MethodToken(importNettyUtils.toLowerCase,importNettyUtils,List(HighLighter(137,31,40)),
            Set(MethodAndLines("getRemoteAddress",List(HighLighter(137,31,66))))),
          MethodToken(importLogger.toLowerCase,importLogger,List(HighLighter(139,5,10),
            HighLighter(150,13,18), HighLighter(154,13,18), HighLighter(160,15,20)),
            Set(MethodAndLines("trace",List(HighLighter(139,5,49), HighLighter(150,13,97))),
              MethodAndLines("error",List(HighLighter(154,13,50), HighLighter(160,15,85))))),
          MethodToken(importRpcReq.toLowerCase,importRpcReq,List(HighLighter(144,31,40)),Set()),
          MethodToken(importChannel.toLowerCase + "future",importChannel + "Future",
            List(HighLighter(148,15,20), HighLighter(153,27,32), HighLighter(154,36,41),
              HighLighter(158,60,65)),Set(MethodAndLines("cause",List(HighLighter(153,27,40),
              HighLighter(154,36,49), HighLighter(158,60,73))), MethodAndLines("isSuccess",
              List(HighLighter(148,15,32))))),
          MethodToken(importChannel.toLowerCase + "futurelistener",
            importChannel + "FutureListener",List(HighLighter(145,11,31)),Set())),0)
    )

    assert(methodTokens ===  testMethodTokens)
  }
}
