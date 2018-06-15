/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.report

import co.cask.cdap.report.util.Constants
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * A utility class to maintain compatibility with Spark1 for reading avro files and aggregating.
  */
object SparkCompat {

  val SPARK_VERSION = "spark1_2.10"

  /**
    * @return the compatible Spark version
    */
  def getSparkVersion: String = {
    return SPARK_VERSION
  }

  /**
    * Reads avro files to construct a [[DataFrame]].
    *
    * @param sql the SQL context to create the [[DataFrame]] with
    * @param inputURIs URIs of the avro files to read
    */
  def readAvroFiles(sql: SQLContext, inputURIs: Seq[String]): DataFrame = {
    sql.read.format("com.databricks.spark.avro").load(inputURIs: _*)
  }

  /**
    * Groups the given [[DataFrame]] by the column [[Constants.RUN]] and aggregates the grouped data
    * by [[ReportAggregationFunction]]
    *
    * @param sql the SQL context from which the [[DataFrame]] is created
    * @param df the [[DataFrame]] to be aggregated
    * @return the aggregated [[DataFrame]]
    */
  def aggregate(sql: SQLContext, df: DataFrame): DataFrame = {
    // Create an aggregation function that aggregates grouped data into a column with data type Record.
    val reportAggFunc = new ReportAggregationFunction()
    // Group the program run meta records by program run Id's and aggregate the grouped data
    // with the aggregation function.
    df.groupBy(Constants.RUN).agg(reportAggFunc(df.columns.map(col): _*).as(ReportGenerationHelper.RECORD_COL))
  }
}
