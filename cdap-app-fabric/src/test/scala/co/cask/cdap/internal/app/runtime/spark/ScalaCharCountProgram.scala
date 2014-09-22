/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.spark.rdd.NewHadoopRDD

/**
 * A simple Spark program written in Scala which counts the number of characters
 */
class ScalaCharCountProgram extends ScalaSparkProgram {

  override def run(sc: SparkContext) {
    // read the dataset
    val inputData: NewHadoopRDD[Array[Byte], String] = sc.readFromDataset("keys", classOf[Array[Byte]], classOf[String])

    // create a new RDD with the same key but the value is the length of the string
    val stringLengths = inputData.map(str => (str._1, Bytes.toBytes(str._2.length)))

    // write to dataset
    sc.writeToDataset(stringLengths, "count", classOf[Array[Byte]], classOf[Array[Byte]])
    sc.getOriginalSparkContext.asInstanceOf[org.apache.spark.SparkContext].stop()
  }
}
