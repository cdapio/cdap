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

package co.cask.cdap.examples.loganalysis

import java.util.concurrent.TimeUnit

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.SparkContext._

/**
 * A spark program which counts the total number of responses for every unique response code
 */
class ResponseCounterProgram extends ScalaSparkProgram {
  private final val LOG: Logger = LoggerFactory.getLogger(classOf[ResponseCounterProgram])

  override def run(context: SparkContext): Unit = {

    val output = context.getRuntimeArguments.get("output")

    val endTime = context.getLogicalStartTime
    val startTime = endTime - TimeUnit.MINUTES.toMillis(60)

    val logsData: NewHadoopRDD[LongWritable, Text] =
      context.readFromStream(LogAnalysisApp.LOG_STREAM, classOf[Text], startTime, endTime)

    val responseCounts = logsData.map(x => (ApacheAccessLog.
      parseFromLogLine(x._2.toString).getResponseCode, 1L)).reduceByKey(_ + _)
      .map(x => (Bytes.toBytes(x._1), Bytes.toBytes(x._2)))

    context.writeToDataset(responseCounts, LogAnalysisApp.RESPONSE_COUNT_STORE,
      classOf[Array[Byte]], classOf[Array[Byte]])

    val ipCounts = logsData.map(x => (ApacheAccessLog.
      parseFromLogLine(x._2.toString).getIpAddress, 1L)).reduceByKey(_ + _)

    context.writeToDataset(ipCounts, LogAnalysisApp.IP_COUNT_STORE,
      classOf[String], classOf[java.lang.Long])
  }
}
