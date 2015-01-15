/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.sparkkmeans

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.NewHadoopRDD
import org.slf4j.{Logger, LoggerFactory}

/**
 * Implementation of KMeans Clustering Spark Program.
 */
class SparkKMeansProgram extends ScalaSparkProgram {
  private final val LOG: Logger = LoggerFactory.getLogger(classOf[SparkKMeansProgram])

  def pointVector(point: Point): Vector[Double] = {
    DenseVector(Array(point.getX, point.getX, point.getZ).map(_.doubleValue()))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  override def run(sc: SparkContext) {

    val args: Array[String] = sc.getRuntimeArguments("args")
    LOG.info("Running with arguments {}", args)
    // Amount of centers to calculate
    val K = if (args.length > 0) args(0).toInt else "2".toInt
    val convergeDist = if (args.length > 1) args(1).toDouble else "0.5".toDouble


    LOG.info("Processing points data")

    val linesDataset: NewHadoopRDD[Array[Byte], Point] =
      sc.readFromDataset("points", classOf[Array[Byte]], classOf[Point])
    val lines = linesDataset.values
    val data = lines.map(pointVector).cache()

    LOG.info("Calculating centers")

    val kPoints = data.takeSample(withReplacement = false, K, 42).toArray
    var tempDist = 1.0
    while (tempDist > convergeDist) {
      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closest.reduceByKey { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
      val newPoints = pointStats.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }
      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      LOG.debug("Finished iteration (delta = {})", tempDist)
    }

    LOG.info("Center count {}", kPoints.size)

    val centers = new Array[(Array[Byte], String)](kPoints.size)
    for (i <- kPoints.indices) {
      LOG.debug("Center {}, {}", i, kPoints(i).toString)
      centers(i) = new Tuple2(i.toString.getBytes, kPoints(i).toArray.mkString(","))
    }

    LOG.info("Writing centers data")

    val originalContext: org.apache.spark.SparkContext = sc.getOriginalSparkContext()
    sc.writeToDataset(originalContext.parallelize(centers), "centers", classOf[Array[Byte]], classOf[String])

    LOG.info("Done!")
  }
}
