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

import co.cask.cdap.api.data.DatasetContext
import co.cask.cdap.api.dataset.lib.KeyValueTable
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import com.google.gson.Gson
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Scala Program to parse CLF format logs.
  */
class ScalaSparkLogParser extends AbstractSpark with SparkMain {

  override protected def configure() = {
    setMainClass(classOf[ScalaSparkLogParser])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val arguments = sec.getRuntimeArguments
    val inputFileSet = arguments.get("input")
    val outputTable = arguments.get("output")

    val logRecord: RDD[(LongWritable, Text)] = sc.fromDataset(inputFileSet)

    val parsedLog = logRecord.map {
      case (ignore, log) => SparkAppUsingGetDataset.parse(log)
    }

    val aggregated = parsedLog
      .reduceByKey {
        case (stats1, stats2) => stats1.aggregate(stats2)
      }
      .mapPartitions(itor => {
        val gson = new Gson();
        itor.map(t => (gson.toJson(t._1), gson.toJson(t._2)))
      })

    // Collect all data to driver and write to dataset directly. That's the intend of the test.
    Transaction((context: DatasetContext) => {
      val kvTable: KeyValueTable = context.getDataset(outputTable)
      aggregated.collectAsMap().foreach(t => {
        kvTable.write(t._1, t._2)
      })
    })
  }
}
