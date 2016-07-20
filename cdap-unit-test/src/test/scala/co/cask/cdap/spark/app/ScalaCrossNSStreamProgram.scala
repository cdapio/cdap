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

import co.cask.cdap.api.Resources
import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext

/**
  * A simple Spark program reading stream from another namespace and write to dataset
  */
class ScalaCrossNSStreamProgram(name: String) extends AbstractSpark with SparkMain {

  def this() = this(classOf[ScalaCrossNSStreamProgram].getSimpleName)

  override protected def configure(): Unit = {
    setName(name)
    setMainClass(classOf[ScalaCrossNSStreamProgram])
    setDescription("Spaarkprogram reading stream from another namespace");
  }

  override def run(implicit sec: SparkExecutionContext) {
    val sparkContext = new SparkContext
    val trainingData = sparkContext.fromStream[String]("streamSpaceForSpark", "testStream", 0, Long.MaxValue)
    val num = trainingData.count()
    trainingData
      .map(x => (Bytes.toBytes(x), Bytes.toBytes(x)))
      .saveAsDataset("count")
  }
}
