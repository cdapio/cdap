/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.examples.wikipedia

import java.util

import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{LDA, LDAModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * Scala program to run Latent Dirichlet Allocation (LDA) on wikipedia data.
 * This is an adaptation of
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/LDAExample.scala
 * for CDAP.
 */
class ScalaSparkLDA extends SparkMain {

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val arguments = sec.getRuntimeArguments
    System.out.println(sec.getSpecification.getProperties);
    println(sec.getSpecification.getProperties)
       System.out.println(arguments);
        println(arguments)

    val inputNamespace = arguments.get("input_namespace")
    val outputNamespace = arguments.get("output_namespace")

    println(s"inputNamespace=$inputNamespace outputNamespace=$outputNamespace")

    // Pre-process data for LDA
    val (corpus, vocabArray, _) = ClusteringUtils.preProcess(
      if (inputNamespace != null) sc.fromDataset(inputNamespace, WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET)
      else sc.fromDataset(WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET),
      sec.getRuntimeArguments.toMap)
    corpus.cache()

    // Run LDA
    val ldaModel = runLDA(corpus, arguments)

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val topics: Array[Array[(String, Double)]] = topicIndices.map {
      case (terms, termWeights) => terms.zip(termWeights).map {
        case (term, weight) => (vocabArray(term.toInt), weight)
      }
    }

    ClusteringUtils.storeResults(sc, sec, topics, outputNamespace,
            WikipediaPipelineApp.SPARK_CLUSTERING_OUTPUT_DATASET)
  }

  private def runLDA(corpus: RDD[(Long, Vector)],
                     arguments: util.Map[String, String]): LDAModel = {
    val k: Int = if (arguments.containsKey("num.topics")) arguments.get("num.topics").toInt else 10
    val maxIterations: Int = if (arguments.containsKey("max.iterations")) arguments.get("max.iterations").toInt else 10
    val lda = new LDA()
    lda.setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(-1)
      .setTopicConcentration(-1)
      .setCheckpointInterval(10)
    lda.run(corpus)
  }
}
