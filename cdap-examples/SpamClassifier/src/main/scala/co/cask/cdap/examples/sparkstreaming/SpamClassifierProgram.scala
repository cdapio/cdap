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

import co.cask.cdap.api.Resources
import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{AbstractSpark, SparkClientContext, SparkExecutionContext, SparkMain}
import com.google.common.base.Strings
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Consumes messages from kafka topics and classify them as spam or ham based on NaiveBayes model.
  * A comma separated list of brokers and topics must be provided as runtime arguments to this program.
  * Kafka messages should be in format of  message-id:message where message-id should be unique. For example
  * message1:hello world
  */
class SpamClassifierProgram(name: String) extends AbstractSpark with SparkMain {

  import SpamClassifierProgram._

  def this() = this(classOf[SpamClassifierProgram].getSimpleName)

  override protected def configure(): Unit = {
    setName(name)
    setMainClass(classOf[SpamClassifierProgram])
    setDescription("Spark Streaming Based Kaka Message Classifier");
    setDriverResources(new Resources(2048))
    setExecutorResources(new Resources(1024))
  }


  override def beforeSubmit(context: SparkClientContext): Unit = {
    context.setSparkConf(new SparkConf().set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=256m"))
  }

  override def run(implicit sec: SparkExecutionContext) {

    val sparkContext = new SparkContext

    LOG.info("Reading stream {} to build classification model", SpamClassifier.STREAM)

    // read the training data stream
    val trainingData = sparkContext.fromStream[String](SpamClassifier.STREAM, 0, Long.MaxValue)

    val termFrequencies = new HashingTF(numFeatures = 1000)

    // spam messages
    val spam = trainingData.filter(_.startsWith("spam")).map(_.split("\t")(1))

    // not spam messages
    val ham = trainingData.filter(_.startsWith("ham")).map(_.split("\t")(1))

    val spamLabeledPoint = spam.map(line => termFrequencies.transform(line.split(" "))).map(x => new LabeledPoint(1, x))
    val hamLabeledPoint = ham.map(line => termFrequencies.transform(line.split(" "))).map(x => new LabeledPoint(0, x))

    val modelData = spamLabeledPoint.union(hamLabeledPoint)
    val model = NaiveBayes.train(modelData, 1.0)

    LOG.info("Built a NaiveBayes model for classification with training data of size: {}", modelData.count())

    val streamingContext = new StreamingContext(sparkContext, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val brokers = sec.getRuntimeArguments.get("kafka.brokers")
    require(!Strings.isNullOrEmpty(brokers), "A comma separated list of kafka brokers must be specified. For example:" +
      " broker1-host:port,broker2-host:port")
    val topics = sec.getRuntimeArguments.get("kafka.topics")
    require(!Strings.isNullOrEmpty(topics), "A comma separated list of kafka topics must be specified. For example: " +
      "topic1,topic2")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    LOG.info("Trying to create a DStream for Kafka with kafka params {} and topics {}", kafkaParams, topicsSet: Any)

    val kafkaData = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](streamingContext,
      kafkaParams, topicsSet)

    // transform the kafka DStream to a key-value pair of message-id and message
    val messages = kafkaData.map(_._2.split(":", 2) match { case Array(x, y) => (x, y) })

    // for each RDD in the DStream transform it a RDD of message-id and label (spam or ham)
    messages.foreachRDD { rdd =>
      rdd.map { line =>
        val vector = termFrequencies.transform(line._2.split(" "))
        val value: Double = model.predict(vector)
        (Bytes.toBytes(line._1), value)
      }.saveAsDataset(SpamClassifier.DATASET)
    }

    streamingContext.start()

    try {
      streamingContext.awaitTermination()
    } catch {
      case _: InterruptedException => streamingContext.stop(true, true)
    }
  }
}

object SpamClassifierProgram {
  private final val LOG: Logger = LoggerFactory.getLogger(classOf[SpamClassifierProgram])
}
