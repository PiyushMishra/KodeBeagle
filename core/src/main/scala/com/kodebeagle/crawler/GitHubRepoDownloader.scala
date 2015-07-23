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

import akka.actor.Actor
import akka.actor.Props
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import com.kodebeagle.logging.Logger
import com.kodebeagle.configuration.KodeBeagleConfig

class GitHubRepoDownloader extends Actor with Logger {

  import GitHubRepoDownloader._
  import GitHubRepoCrawlerApp._

  def receive: PartialFunction[Any, Unit] = {

    case DownloadOrganisationRepos(organisation) => downloadFromOrganization(organisation)

    case DownloadPublicRepos(since, zipOrClone) =>
      try {
        val nextSince = downloadFromRepoIdRange(since, zipOrClone)
        self ! DownloadPublicRepos(nextSince,zipOrClone)
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          log.error("Got Exception [" + ex.getMessage + "] Trying to download, " +
            "waiting for other tokens")
          self ! DownloadPublicRepos(since, zipOrClone)
      }

    case RateLimit(rateLimit) =>
      log.debug(s"rate Limit Remaining is : $rateLimit")
      if (rateLimit == "0") {
        GitHubApiHelper.token = KodeBeagleConfig.nextToken()
        log.info("limit 0,token changed :" + GitHubApiHelper.token)
      }
  }

}

object GitHubRepoDownloader {

  case class DownloadOrganisationRepos(organisation: String)

  case class DownloadPublicRepos(since: Int, zipOrClone: String)

  case class RateLimit(limit: String)

  val system = ActorSystem("RepoDownloder")

  val repoDownloader = system.actorOf(Props[GitHubRepoDownloader])

  val zipActor = system.actorOf(Props[ZipActor])

}

class ZipActor extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case filePath: String => ZipUtil.createZip(filePath, filePath + ".zip")
      import sys.process._
      Process("rm -fr " + filePath).!!
  }
}

