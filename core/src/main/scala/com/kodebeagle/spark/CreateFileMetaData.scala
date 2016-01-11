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

import java.util

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.javaparser.{SingleClassBindingResolver, JavaASTParser}
import com.kodebeagle.javaparser.JavaASTParser.ParseType
import com.kodebeagle.spark.SparkIndexJobHelper._
import com.kodebeagle.logging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.eclipse.jdt.core.dom.{CompilationUnit, ASTNode}
import org.elasticsearch.spark._

case class RepoSource(repoId: Long, fileName: String, fileContent: String)
case class ExternalRef(id: Int,fqt:String)
case class VarTypeLocation(loc: String, id: Int)
case class MethodTypeLocation(loc: String, id: Int, method: String, argTypes: List[String])
case class MethodDefinition(loc: String, method: String, argTypes: List[String])
case class InternalRef(childLine:String, parentLine:String)
case class FileMetaData(repoId: Long,fileName:String, fileTypes :util.List[String],externalRefList: List[ExternalRef],
                        typeLocationList: List[VarTypeLocation],methodTypeLocation: List[MethodTypeLocation],
                        methodDefinitionList: List[MethodDefinition], internalRefList: List[InternalRef])

object CreateFileMetaData extends Logger{
  val esPortKey = "es.port"
  val esNodesKey = "es.nodes"
  val jobName = "FileMetaData"

  def main (args: Array[String]) {
    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster).setAppName(jobName)
    conf.set(esNodesKey, KodeBeagleConfig.esNodes)
    conf.set(esPortKey, KodeBeagleConfig.esPort)
    val sc: SparkContext = createSparkContext(conf)
    sc.setCheckpointDir(KodeBeagleConfig.sparkCheckpointDir)

    val repoSources = sc.esRDD("sourcefile/typesourcefile").map({
      case (repoId, valuesMap) => {
        RepoSource (valuesMap.get("repoId").getOrElse(0).asInstanceOf[Int],
          valuesMap.get("fileName").getOrElse("").asInstanceOf[String],
          valuesMap.get("fileContent").getOrElse("").toString)
      }
    })

    val filesMetaData = getFilesMetaData(repoSources.filter(_.fileName.endsWith(".java")))
    filesMetaData.map(a => toJson(a)).saveAsTextFile("/opt/temp_filemetadata/temp")
  }

  import scala.collection.JavaConversions._

  def getFilesMetaData(repoSources: RDD[RepoSource]) = {
    val filesMetaData = repoSources.map { source =>
      val pars: JavaASTParser = new JavaASTParser(true)
      val cu: ASTNode = pars.getAST(source.fileContent, ParseType.COMPILATION_UNIT)
      if (cu != null) {
        val unit: CompilationUnit = cu.asInstanceOf[CompilationUnit]
        val resolver: SingleClassBindingResolver = new SingleClassBindingResolver(unit)
        resolver.resolve
        val typesAtPos = resolver.getVariableTypesAtPosition
        //External reference
        val externalRefs = scala.collection.mutable.Set[String]()
        for (e <- resolver.getTypesAtPosition.entrySet) {
          val line: Integer = unit.getLineNumber(e.getKey)
          val col: Integer = unit.getColumnNumber(e.getKey)
          externalRefs.add(e.getValue.toString)
        }
        val idVsExternalRefs = externalRefs.zipWithIndex.toMap
        val externalRefsList = idVsExternalRefs.map(x => ExternalRef(x._2, x._1))
        // typeLocationList for variables
        val typeLocationVarList = getTypeLocationVarList(unit, typesAtPos, idVsExternalRefs)
        // typelocation for method call expression
        val typeLocationMethodList =
          getTypeLocationMethodList(unit, resolver, idVsExternalRefs)

        // method definition in that class
        val methodDefinitionList = for (m <- resolver.getDeclaredMethods) yield {
          val line: Integer = unit.getLineNumber(m.getLocation)
          MethodDefinition(line.toString, m.getMethodName, m.getArgTypes.toList)
        }

        //internal references
        val internalRefsList = getInternalRefs(unit, resolver)
        Some(FileMetaData(source.repoId, source.fileName, resolver.getClassesInFile, externalRefsList.toList,
          typeLocationVarList.toList, typeLocationMethodList.toList, methodDefinitionList.toList,
          internalRefsList.toList))
      } else {
        log.info("Unable to create AST for file " + source.fileName + "and file contents are \n" + source.fileContent)
        None
      }
    }
    filesMetaData.filter(_.isDefined)
  }

  def getTypeLocationVarList(unit: CompilationUnit, typesAtPos: util.Map[ASTNode, String],
                             idVsExternalRefs: Map[String, Int]): scala.collection.mutable.Set[VarTypeLocation] = {
    for (e <- typesAtPos.entrySet) yield {
      val line: Integer = unit.getLineNumber(e.getKey.getStartPosition)
      val col: Integer = unit.getColumnNumber(e.getKey.getStartPosition)
      val valueType = e.getValue
      VarTypeLocation(line + "#" + col + "#" + e.getKey.getLength, idVsExternalRefs.getOrElse(valueType, -1))
    }
  }

  def getTypeLocationMethodList(unit: CompilationUnit, resolver: SingleClassBindingResolver,
                                idVsExternalRefs: Map[String, Int]): scala.collection.mutable.Set[MethodTypeLocation] = {
    for {entry <- resolver.getMethodInvoks.entrySet
         m <- entry.getValue} yield {
      val loc: Integer = m.getLocation
      val line: Integer = unit.getLineNumber(loc)
      val col: Integer = unit.getColumnNumber(loc)
      MethodTypeLocation(line + "#" + col, idVsExternalRefs.getOrElse(m.getTargetType, -1),
        m.getMethodName, m.getArgTypes.toList)
    }
  }

  def getInternalRefs(unit: CompilationUnit, resolver: SingleClassBindingResolver): scala.collection.mutable.Set[InternalRef] = {
    for (e <- resolver.getVariableDependencies.entrySet) yield {
      val child: ASTNode = e.getKey
      val chline: Integer = unit.getLineNumber(child.getStartPosition)
      val chcol: Integer = unit.getColumnNumber(child.getStartPosition)
      val chlength: Integer = child.getLength
      val parent: ASTNode = e.getValue
      val pline: Integer = unit.getLineNumber(parent.getStartPosition)
      val pcol: Integer = unit.getColumnNumber(parent.getStartPosition)
      InternalRef(chline + "#" + chcol + "#" + chlength, pline + "#" + pcol)
    }
  }
}
