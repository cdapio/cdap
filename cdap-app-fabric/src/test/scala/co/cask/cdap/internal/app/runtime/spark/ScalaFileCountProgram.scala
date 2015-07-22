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

package co.cask.cdap.internal.app.runtime.spark

import java.util

import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.spark.rdd.NewHadoopRDD

/**
 * A simple Spark program written in Scala which counts the number of characters
 */
class ScalaFileCountProgram extends ScalaSparkProgram {

  override def run(sc: SparkContext) {
    // determine input and output dataset names
    val input = sc.getRuntimeArguments.get("input")
    val output = sc.getRuntimeArguments.get("output")

    // read the dataset
    val inputData: NewHadoopRDD[Long, String] = sc.readFromDataset(input, classOf[Long], classOf[String])

    // create a new RDD with the same key but the value is the length of the string
    val stringLengths = inputData.map(str => (str._2, str._2.length))

    // write to dataset
    sc.writeToDataset(stringLengths, output, classOf[String], classOf[Integer])

    val inputPartitionTime = sc.getRuntimeArguments.get("inputKey")
    val outputPartitionTime = sc.getRuntimeArguments.get("outputKey")

    if (inputPartitionTime != null && outputPartitionTime != null) {
      val inputArgs: util.HashMap[String, String] = new util.HashMap[String, String]
      TimePartitionedFileSetArguments.setInputStartTime(inputArgs, inputPartitionTime.toLong - 100)
      TimePartitionedFileSetArguments.setInputEndTime(inputArgs, inputPartitionTime.toLong + 100)


      // read the dataset with custom user dataset arguments
      val customInputData: NewHadoopRDD[Long, String] = sc.readFromDataset(input, classOf[Long], classOf[String], inputArgs)

      // create a new RDD with the same key but the value is the length of the string
      val customPartitionStringLengths = customInputData.map(str => (str._2, str._2.length))

      // write the character count to dataset with user custom dataset args
      val outputArgs: util.HashMap[String, String] = new util.HashMap[String, String]
      TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, outputPartitionTime.toLong)
      sc.writeToDataset(customPartitionStringLengths, output, classOf[String], classOf[Integer], outputArgs)
    }
  }
}
