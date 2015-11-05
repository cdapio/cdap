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

import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector

/**
 * Scala program to run K-Means algorithm on Wikipedia data.
 */
class ScalaSparkKMeans extends ScalaSparkProgram {
  override def run(context: SparkContext): Unit = {
    val arguments: util.Map[String, String] = context.getRuntimeArguments
    val maxIterations: Int = if (arguments.containsKey("max.iterations")) arguments.get("max.iterations").toInt else 10
    val k: Int = if (arguments.containsKey("num.topics")) arguments.get("num.topics").toInt else 10

    // Pre-process data for LDA
    val (corpus, vocabArray, _) = ClusteringUtils.preProcess(context)
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
        case element: ((Double, Int)) => ((vocabArray(element._2), element._1))
      }
    }

    ClusteringUtils.storeResults(context, topTenTermsWithWeights, WikipediaPipelineApp.SPARK_CLUSTERING_OUTPUT_DATASET)
  }
}
