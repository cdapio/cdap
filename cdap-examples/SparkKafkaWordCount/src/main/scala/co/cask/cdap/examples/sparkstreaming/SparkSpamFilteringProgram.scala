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
package co.cask.cdap.examples.sparkstreaming

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Consumes messages from kafka topics and performs simple word count of messages.
  * A comma separated list of brokers and topics must be provided as runtime arguments to this program.
  */
class SparkSpamFilteringProgram extends SparkMain {

  override def run(implicit sec: SparkExecutionContext) {

    val sparkContext: SparkContext = new SparkContext

    // read the spam stream
    val trainingData: RDD[String] = sparkContext.fromStream(SparkSpamFiltering.STREAM, 0, Long.MaxValue)
    val tf = new HashingTF(numFeatures = 1000)

    var spam: RDD[String] = trainingData.filter(x => x.startsWith("spam"))
    spam = spam.map { line =>
      val strings = line.split("\t")
      strings(1)
    }

    var notSpam: RDD[String] = trainingData.filter(x => x.startsWith("ham"))
    notSpam = notSpam.map { line =>
      val strings = line.split("\t")
      strings(1)
    }

    val spamLabeledPoint: RDD[LabeledPoint] = spam.map(line => tf.transform(line.split(" "))).map(x => new LabeledPoint
    (1, x))
    val notSpamLabeledPoint: RDD[LabeledPoint] = notSpam.map(line => tf.transform(line.split(" "))).map(x =>
      new LabeledPoint
      (0, x))

    val modelData: RDD[LabeledPoint] = spamLabeledPoint.union(notSpamLabeledPoint)

    println(modelData.take(10).deep.mkString("\n"))

    val model: NaiveBayesModel = NaiveBayes.train(modelData, 1.0)

    val streamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(2))

    val brokers: String = sec.getRuntimeArguments.get("kafka.brokers")
    //    Preconditions.checkNotNull(brokers, "A comma separated list of kafka brokers must be specified. " +
    //      "For example: broker1-host:port,broker2-host:port")

    val topics: String = sec.getRuntimeArguments.get("kafka.topics")
    //    Preconditions.checkNotNull(brokers, "A comma separated list of kafka topics must be specified. " +
    //      "For example: topic1,topic2")


    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext,
      kafkaParams, topicsSet)

    messages.print()

    messages.foreachRDD { (rdd: RDD[(String, String)]) =>
      rdd.map { line =>
        val vector: Vector = tf.transform(line._2.split(" "))
        val value: Double = model.predict(vector)
        (Bytes.toBytes(line._1), value)
      }.saveAsDataset(SparkSpamFiltering.DATASET)
    }

    streamingContext.start()

    try {
      streamingContext.awaitTermination()
    } catch {
      case _: InterruptedException => streamingContext.stop(true, true)
    }
  }
}
