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

import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector

import scala.collection.JavaConversions._

/**
 * Scala program to run K-Means algorithm on Wikipedia data.
 */
class ScalaSparkKMeans extends SparkMain {

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val arguments = sec.getRuntimeArguments.toMap
    val maxIterations = arguments.get("max.iterations").map(_.toInt).getOrElse(10)
    val k = arguments.get("num.topics").map(_.toInt).getOrElse(10)

    var dataNamespace = sec.getRuntimeArguments.get(WikipediaPipelineApp.NAMESPACE_ARG)
    dataNamespace = if (dataNamespace != null) dataNamespace else sec.getNamespace

    // Pre-process data for LDA
    val (corpus, vocabArray, _) = ClusteringUtils.preProcess(
      sc.fromDataset(dataNamespace, WikipediaPipelineApp.NORMALIZED_WIKIPEDIA_DATASET), arguments)
    corpus.cache()

    // Cluster the data into two classes using KMeans
    val vectors = corpus.map {
      case (id, vector) => vector
    }

    val kMeans = new KMeans()
    kMeans.setK(k)
    kMeans.setMaxIterations(maxIterations)

    val kMeansModel = kMeans.run(vectors)

    val topTenWeightsWithIndex: Array[Array[(Double, Int)]] = kMeansModel.clusterCenters.map {
      case vector: Vector => vector.toArray.zipWithIndex.sorted.reverse.take(10)
    }

    val topTenTermsWithWeights: Array[Array[(String, Double)]] = topTenWeightsWithIndex.map {
      case clusterCenter: Array[(Double, Int)] => clusterCenter.map {
        case element: ((Double, Int)) => (vocabArray(element._2), element._1)
      }
    }

    ClusteringUtils.storeResults(sc, sec, topTenTermsWithWeights, dataNamespace,
      WikipediaPipelineApp.SPARK_CLUSTERING_OUTPUT_DATASET)
  }
}
