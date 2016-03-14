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

package co.cask.cdap.admin

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.spark.rdd.NewHadoopRDD

/**
 * A Scala Spark program that counts the number of words and truncates its output before writing
 */
class ScalaAdminSparkProgram extends ScalaSparkProgram {

  override def run(sc: SparkContext) {
    // read the dataset
    val input: NewHadoopRDD[Array[Byte], Array[Byte]] =
      sc.readFromDataset("lines", classOf[Array[Byte]], classOf[Array[Byte]])

    val result = input
      .values
      .flatMap(line => Bytes.toString(line).split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(pair => (Bytes.toBytes(pair._1), Bytes.toBytes(pair._2)))

    // truncate output dataset before writing
    sc.getAdmin.truncateDataset("counts")

    // write to dataset
    sc.writeToDataset(result, "counts", classOf[Array[Byte]], classOf[Array[Byte]])
  }
}
