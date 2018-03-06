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
package co.cask.cdap.examples.report

import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoders, Row, SQLContext, SparkSession}

class ReportGenerationSparkScala extends SparkMain {
  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext
    val spark = SparkSession.builder().master("local").getOrCreate()
    val groupedData = spark.read.format("com.databricks.spark.avro")
      .load("/Users/Chengfeng/tmp/run_meta.avro")
      .groupBy("program", "run")
    val agg = new Aggregator[Row, RecordBuilder, Record] {
      def zero: RecordBuilder = new RecordBuilder()
      def reduce(b: RecordBuilder, a: Row): RecordBuilder = {
        print(a.getAs("run"))
        b.setProgramRunStatus(a.getString(1), a.getString(2))
        b
      }
      def merge(b1: RecordBuilder, b2: RecordBuilder) = b1.merge(b2)
      def finish(b: RecordBuilder): Record = b.build()
      def bufferEncoder()= Encoders.kryo[RecordBuilder]; def outputEncoder()= Encoders.kryo[Record]}.toColumn
    groupedData
      .agg(rowAgg)
      .select(rowAgg)
      .write.json("/Users/Chengfeng/Downloads/cdap-sandbox-5.0.0-SNAPSHOTsers/Chengfeng/tmp/report/reportId.json")
  }
}
