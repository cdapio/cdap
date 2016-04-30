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

import java.util.HashMap
import java.util.concurrent.TimeUnit

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

/**
 * A spark program which counts the total number of responses for every unique response code
 */
class ResponseCounterProgram extends SparkMain {

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val endTime = sec.getLogicalStartTime
    val startTime = endTime - TimeUnit.MINUTES.toMillis(60)

    val logsData: RDD[String] = sc.fromStream(LogAnalysisApp.LOG_STREAM, startTime, endTime)
    val parsedLogs: RDD[ApacheAccessLog] = logsData.map(x => ApacheAccessLog.parseFromLogLine(x))

    parsedLogs
      .map(x => (x.getResponseCode, 1L))
      .reduceByKey(_ + _)
      .map(x => (Bytes.toBytes(x._1), Bytes.toBytes(x._2)))
      .saveAsDataset(LogAnalysisApp.RESPONSE_COUNT_STORE)

    val outputArgs = new HashMap[String, String]()
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, endTime)
    parsedLogs
      .map(x => (x.getIpAddress, 1L))
      .reduceByKey(_ + _)
      .saveAsDataset(LogAnalysisApp.REQ_COUNT_STORE, outputArgs.toMap)
  }
}

/**
  * Companion object for holding static fields
  */
object ResponseCounterProgram {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[ResponseCounterProgram])
}