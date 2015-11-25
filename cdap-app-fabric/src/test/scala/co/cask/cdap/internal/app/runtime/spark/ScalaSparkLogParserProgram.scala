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

import co.cask.cdap.api.dataset.lib.KeyValueTable
import co.cask.cdap.api.spark.{SparkContext, ScalaSparkProgram}
import co.cask.cdap.internal.app.runtime.spark.SparkAppUsingGetDataset.{LogStats, LogKey}
import com.google.gson.Gson
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.rdd.{RDD, NewHadoopRDD}

/**
 * Scala Program to parse CLF format logs.
 */
class ScalaSparkLogParserProgram extends ScalaSparkProgram {
  val GSON: Gson = new Gson()

  override def run(context: SparkContext): Unit = {
    val arguments: util.Map[String, String] = context.getRuntimeArguments
    val inputFileSet: String = arguments.get("input")
    val outputTable: String = arguments.get("output")

    val logRecord: NewHadoopRDD[LongWritable, Text] =
      context.readFromDataset(inputFileSet, classOf[LongWritable], classOf[Text])

    val parsedLog: RDD[(LogKey, LogStats)] = logRecord.map {
      case (ignore, log) => SparkAppUsingGetDataset.parse(log)
    }

    val aggregate: RDD[(LogKey, LogStats)] = parsedLog.reduceByKey {
      case (stats1, stats2) => stats1.aggregate(stats2)
    }

    val kvTable: KeyValueTable = context.getDataset(outputTable)
    aggregate.collect().foreach {
      case (logKey, logStats) => kvTable.write(GSON.toJson(logKey), GSON.toJson(logStats))
    }
  }
}
