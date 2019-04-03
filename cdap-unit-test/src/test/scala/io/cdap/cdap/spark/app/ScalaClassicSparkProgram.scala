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

import org.apache.spark.SparkContext

/**
  * A Spark program that has a static main method instead of extending from [[co.cask.cdap.api.spark.SparkMain]].
  */
object ScalaClassicSparkProgram {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    require(sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).reduce(_ + _) == 55)
  }
}
