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

/**
  * Change ScalaCharCountProgram to read from another namespace
  */
class ScalaCrossNSDatasetProgram extends AbstractSpark with SparkMain {

  override protected def configure() = {
    setName(classOf[ScalaCrossNSDatasetProgram].getSimpleName)
    setDescription("Use Objectstore dataset as input job")
    setMainClass(classOf[ScalaCrossNSDatasetProgram])
  }

  override def run(implicit sec: SparkExecutionContext) = {
    val sc = new SparkContext

    // read the dataset
    val inputData: RDD[(Array[Byte], String)] = sc.fromDataset("datasetSpaceForSpark", "keys")

    // create a new RDD with the same key but the value is the length of the string
    inputData
      .map(str => (str._1, Bytes.toBytes(str._2.length)))
      .saveAsDataset("count")
  }
}
