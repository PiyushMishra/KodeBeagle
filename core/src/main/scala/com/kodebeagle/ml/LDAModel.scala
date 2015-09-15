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

package com.kodebeagle.ml

import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue

/**
 *
 * Latent Dirichlet Allocation (LDA) model.
 *
 * This abstraction permits for different underlying representations,
 * including local and distributed data structures.
 */
abstract class LDAModel private[ml] {

  /** Number of topics */
  def k: Int

  /** Vocabulary size (number of terms or terms in the vocabulary) */
  def vocabSize: Long

  /**
   * Return the topics described by weighted terms.
   *
   * This limits the number of terms per topic.
   * This is approximate; it may not return exactly the top-weighted terms for each topic.
   * To get a more precise set of top terms, increase maxTermsPerTopic.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term index).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(maxTermsPerTopic: Long): Array[Array[(Int, Long)]]
  
  def summarizeDocGroups(groupIndex: Int): Array[(Long, Long, Double)]

  /** 
   * Return the topics described by weighted terms.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term index).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(): Array[Array[(Int, Long)]] = describeTopics(vocabSize)

}

/**
 *
 * Distributed LDA model.
 * This model stores the inferred topics, the full training dataset, and the topic distributions.
 *
 */
class DistributedLDAModel private[ml] (
    private val state: LDA.LearningState) extends LDAModel {

  override def k: Int = state.k

  override def vocabSize: Long = state.vocabSize()

  override def describeTopics(maxTermsPerTopic: Long): Array[Array[(Int, Long)]] = {
    state.describeTopics(maxTermsPerTopic)
  }
  
  override def summarizeDocGroups(groupIndex: Int=0): Array[(Long, Long, Double)] = {
    state.summarizeDocGroups(groupIndex)
  }

  /**
   * Compute the log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, alpha, eta)
   *
   * Note:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   */
  def logLikelihood: Double = state.logLikelihood()

  /**
   * Compute the log probability of the current parameter estimate, under the prior:
   *  log P(topics, topic distributions for docs | alpha, eta)
   */
  def logPrior: Double = state.logPrior()

}
