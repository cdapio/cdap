/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.spark.app

import io.cdap.cdap.api.common.Bytes
import io.cdap.cdap.api.dataset.lib.TimeseriesTable
import io.cdap.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}

import scala.collection.convert.ImplicitConversions.`map AsScala`


/**
  * A testing Spark program for testing Spark streaming in CDAP.
  * It does a running word count over stream of text.
  */
class KafkaSparkStreaming extends AbstractSpark with SparkMain {

  override protected def configure() = {
    setMainClass(classOf[KafkaSparkStreaming])
  }

  override def run(implicit sec: SparkExecutionContext) = {
    val args: Map[String, String] = sec.getRuntimeArguments.toMap
    val ssc = StreamingContext.getOrCreate(args("checkpoint.path"), () => createStreamingContext(args))

    ssc.start()

    try {
      ssc.awaitTermination()
    } catch {
      case _: InterruptedException => ssc.stop(true, true)
    }
  }

  private def createStreamingContext(args: Map[String, String])
                                    (implicit sec: SparkExecutionContext) : StreamingContext = {
    val ssc = new StreamingContext(new SparkContext, Seconds(1))
    ssc.checkpoint(args("checkpoint.path"))

    // Expect the result dataset a timeseries table
    val resultDataset = args("result.dataset")
    val topics = args("kafka.topics").split(",").toSet
    val kafkaParams = Map(("metadata.broker.list", args("kafka.brokers")),
      ("auto.offset.reset", "smallest"))
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, consumerStrategy)

    kafkaDStream
      .map(_.value())
      .flatMap(_.split("\\s+"))
      .map((_, 1L))
      .reduceByKey(_ + _)
      .foreachRDD((rdd, time) => {
        rdd
          .map(t => {
            // Key is ignored when writing to TS table
            val sec = time.milliseconds / 1000
            val entry = new TimeseriesTable.Entry(Bytes.toBytes(t._1), Bytes.toBytes(t._2), sec)
            (Bytes.toBytes(sec), entry)
          })
          .saveAsDataset(resultDataset)
      })

    ssc
  }
}
