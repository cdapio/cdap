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
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

/**
  * A simple Spark program written in Scala which counts the number of characters. It reads from a dataset and writes
  * to another dataset. These datasets can be in another namespace and their names should be specified as runtime
  * arguments. If no runtime arguments are given it will look for these dataset in its own namespace
  */
class ScalaCharCountProgram extends AbstractSpark with SparkMain {

  override protected def configure() = {
    setName(classOf[ScalaCharCountProgram].getSimpleName)
    setDescription("Use Objectstore dataset as input job")
    setMainClass(classOf[ScalaCharCountProgram])
  }

  override def run(implicit sec: SparkExecutionContext) = {
    val sc = new SparkContext
    val arguments = sec.getRuntimeArguments.asScala
    val inputDSNamespace = arguments.getOrElse(ScalaCharCountProgram.INPUT_DATASET_NAMESPACE, sec.getNamespace)
    val inputDSName = arguments.getOrElse(ScalaCharCountProgram.INPUT_DATASET_NAME, "keys")
    val outputDSNamespace = arguments.getOrElse(ScalaCharCountProgram.OUTPUT_DATASET_NAMESPACE, sec.getNamespace)
    val outputDSName = arguments.getOrElse(ScalaCharCountProgram.OUTPUT_DATASET_NAME, "count")

    // read the dataset
    val inputData: RDD[(Array[Byte], String)] = sc.fromDataset(inputDSNamespace, inputDSName)

    // create a new RDD with the same key but the value is the length of the string
    inputData
      .map(str => (str._1, Bytes.toBytes(str._2.length)))
      .saveAsDataset(outputDSNamespace, outputDSName)
  }
}

object ScalaCharCountProgram {
  val INPUT_DATASET_NAMESPACE = "input.dataset.namespace"
  val INPUT_DATASET_NAME = "input.dataset.name"
  val OUTPUT_DATASET_NAMESPACE = "output.dataset.namespace"
  val OUTPUT_DATASET_NAME = "output.dataset.name"
}
