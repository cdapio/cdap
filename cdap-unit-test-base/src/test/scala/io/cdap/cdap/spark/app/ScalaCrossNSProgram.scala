/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.hadoop.io.{LongWritable, Text}
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

    val inputNS = arguments.getOrElse(ScalaCrossNSProgram.INPUT_NAMESPACE, sec.getNamespace)
    val inputName = arguments.getOrElse(ScalaCrossNSProgram.INPUT_NAME, "input")

    val outputNS = arguments.getOrElse(ScalaCrossNSProgram.OUTPUT_NAMESPACE, sec.getNamespace)
    val outputName = arguments.getOrElse(ScalaCrossNSProgram.OUTPUT_NAME, "count")

    val sparkContext = new SparkContext
    val trainingData = sparkContext.fromDataset[LongWritable, Text](inputNS, inputName).values.map(t => t.toString)
    val num = trainingData.count()
    trainingData
      .map(x => (Bytes.toBytes(x), Bytes.toBytes(x)))
      .saveAsDataset(outputNS.toString, outputName.toString)
  }
}

object ScalaCrossNSProgram {
  val INPUT_NAMESPACE = "input.namespace"
  val INPUT_NAME = "input.name"
  val OUTPUT_NAMESPACE = "output.namespace"
  val OUTPUT_NAME = "output.name"
}
