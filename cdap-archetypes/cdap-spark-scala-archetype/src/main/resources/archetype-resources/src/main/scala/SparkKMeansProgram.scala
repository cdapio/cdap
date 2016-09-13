/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
 *
 * This example is based on the Apache Spark Example SparkKMeans. The original file may be found at
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkKMeans.scala
 *
 * Copyright 2014 The Apache Software Foundation. Licensed under the Apache License, Version 2.0.
 *
 */

package $package

/**
 * Implementation of KMeans Clustering Spark Program.
 */
class SparkKMeansProgram extends SparkMain {

  import SparkKMeansProgram._

  override def run(implicit sec: SparkExecutionContext) {
    val sc = new SparkContext
    val arguments = Option(sec.getRuntimeArguments.get("args"))
    val args = arguments.map(_.split("\\s")).getOrElse(Array())

    LOG.info("Running with arguments {}", args)
    // Amount of centers to calculate
    val k = if (args.nonEmpty) args(0).toInt else 2
    val convergeDist = if (args.length > 1) args(1).toDouble else 0.5d

    LOG.info("Processing points data")

    val linesDataset: RDD[(Array[Byte], Point)] = sc.fromDataset("points")
    val lines = linesDataset.values
    val data = lines.map(pointVector).cache()

    LOG.info("Calculating centers")

    val kPoints = data.takeSample(withReplacement = false, k, 42)
    var tempDist = 1.0
    while (tempDist > convergeDist) {
      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closest.reduceByKey { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
      val newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()
      tempDist = 0.0
      for (i <- 0 until k) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      LOG.debug("Finished iteration (delta = {})", tempDist)
    }

    LOG.info("Center count {}", kPoints.length)

    LOG.info("Writing centers data")
    sc.parallelize(kPoints.zipWithIndex
      .map(p => {(Bytes.toBytes(p._2), p._1.toArray.mkString(","))}))
      .saveAsDataset("centers")

    LOG.info("Done!")
  }
}

object SparkKMeansProgram {

  private final val LOG: Logger = LoggerFactory.getLogger(classOf[SparkKMeansProgram])

  private def pointVector(point: Point): Vector[Double] = {
    DenseVector(Array(point.getX, point.getX, point.getZ).map(_.doubleValue()))
  }

  private def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }
}
