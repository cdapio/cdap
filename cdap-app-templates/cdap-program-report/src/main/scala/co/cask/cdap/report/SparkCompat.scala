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
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * A utility class to maintain compatibility with Spark2 for reading avro files and aggregating.
  */
object SparkCompat {

  val SPARK_VERSION = "spark2_2.11"

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
    sql.sparkSession.read.format("com.databricks.spark.avro").load(inputURIs: _*)
  }

  /**
    * Groups the given [[DataFrame]] by the column [[Constants.RUN]] and aggregates the grouped data
    * by [[RecordAggregator]]
    *
    * @param sql the SQL context from which the [[DataFrame]] is created
    * @param df the [[DataFrame]] to be aggregated
    * @return the aggregated [[DataFrame]]
    */
  def aggregate(sql: SQLContext, df: DataFrame): DataFrame = {
    import sql.sparkSession.implicits._
    // Create an aggregator that aggregates grouped data into a column with data type Record.
    val aggCol = new RecordAggregator().toColumn.alias(ReportGenerationHelper.RECORD_COL).as[Record]
    // Group the program run meta records by program run Id's and aggregate the grouped data with aggCol.
    df.groupBy(Constants.RUN).agg(aggCol)
  }
}
