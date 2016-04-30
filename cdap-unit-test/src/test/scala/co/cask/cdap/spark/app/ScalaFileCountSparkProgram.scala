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

import java.util

import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * A simple Spark program written in Scala which counts the number of characters
  */
class ScalaFileCountSparkProgram extends AbstractSpark with SparkMain {

  override protected def configure() = {
    setMainClass(classOf[ScalaFileCountSparkProgram])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    // determine input and output dataset names
    val input = sec.getRuntimeArguments.get("input")
    val output = sec.getRuntimeArguments.get("output")

    // read the dataset
    val inputData: RDD[(Long, String)] = sc.fromDataset(input)

    // create a new RDD with the same key but the value is the length of the string and write to dataset
    inputData.values
      .map(str => (str, str.length))
      .saveAsDataset(output)

    val inputPartitionTime = sec.getRuntimeArguments.get("inputKey")
    val outputPartitionTime = sec.getRuntimeArguments.get("outputKey")

    if (inputPartitionTime != null && outputPartitionTime != null) {
      val inputArgs = new util.HashMap[String, String]
      TimePartitionedFileSetArguments.setInputStartTime(inputArgs, inputPartitionTime.toLong - 100)
      TimePartitionedFileSetArguments.setInputEndTime(inputArgs, inputPartitionTime.toLong + 100)

      // write the character count to dataset with user custom dataset args
      val outputArgs: util.HashMap[String, String] = new util.HashMap[String, String]
      TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, outputPartitionTime.toLong)

      // read the dataset with custom user dataset arguments
      val customInputData: RDD[(Long, String)] = sc.fromDataset(input, inputArgs.toMap)

      // create a new RDD with the same key but the value is the length of the string
      customInputData.values
        .map(str => (str, str.length))
        .saveAsDataset(output, outputArgs.toMap)
    }
  }
}
