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

package co.cask.cdap.examples.sparkloganalysis

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.slf4j.{Logger, LoggerFactory}
/**
 * A spark program which counts the total number of responses for every unique response code
 */
class ResponseCounterProgram extends ScalaSparkProgram {
  private final val LOG: Logger = LoggerFactory.getLogger(classOf[ResponseCounterProgram])

  override def run(context: SparkContext): Unit = {

    val logsData: NewHadoopRDD[LongWritable, Text] =
      context.readFromStream(LogAnalysisApp.LOG_STREAM, classOf[Text])

    val apacheLogs: RDD[ApacheAccessLog] = logsData.map(x => ApacheAccessLog.
      parseFromLogLine(x._2.toString)).cache()

    val responseCounts = logsData.map(x => (ApacheAccessLog.
      parseFromLogLine(x._2.toString).getResponseCode, 1L)).reduceByKey(_ + _)
      .map(x => (Bytes.toBytes(x._1), Bytes.toBytes(x._2)))

    context.writeToDataset(responseCounts, LogAnalysisApp.RESPONSE_COUNT_STORE,
      classOf[Array[Byte]], classOf[Array[Byte]])
  }
}
