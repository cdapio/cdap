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

package co.cask.cdap.spark.app

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

/**
  * A simple Spark program which can read a stream and writes to a dataset in another namespace if one has been
  * provided through runtime arguments arguments else it will look for stream/dataset in its own namespace.
  */
class ScalaCrossNSProgram(name: String) extends AbstractSpark with SparkMain {

  def this() = this(classOf[ScalaCrossNSProgram].getSimpleName)

  override protected def configure(): Unit = {
    setName(name)
    setMainClass(classOf[ScalaCrossNSProgram])
    setDescription("Spark program reading stream from another namespace");
  }

  override def run(implicit sec: SparkExecutionContext) {
    val arguments = sec.getRuntimeArguments.asScala
    val streamNamespace = arguments.getOrElse(ScalaCrossNSProgram.STREAM_NAMESPACE, sec.getNamespace)
    val streamName = arguments.getOrElse(ScalaCrossNSProgram.STREAM_NAME, "testStream")
    val datasetNamespace = arguments.getOrElse(ScalaCrossNSProgram.DATASET_NAMESPACE, sec.getNamespace)
    val datasetName = arguments.getOrElse(ScalaCrossNSProgram.DATASET_NAME, "count")

    val sparkContext = new SparkContext
    val trainingData = sparkContext.fromStream[String](streamNamespace.toString, streamName.toString, 0, Long.MaxValue)
    val num = trainingData.count()
    trainingData
      .map(x => (Bytes.toBytes(x), Bytes.toBytes(x)))
      .saveAsDataset(datasetNamespace.toString, datasetName.toString)
  }
}

object ScalaCrossNSProgram {
  val STREAM_NAMESPACE = "stream.namespace"
  val STREAM_NAME = "stream.name"
  val DATASET_NAMESPACE = "dataset.namespace"
  val DATASET_NAME = "dataset.name"
}
