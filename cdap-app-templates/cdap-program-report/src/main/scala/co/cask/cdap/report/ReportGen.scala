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

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}

class ReportGen(private val spark: SparkSession) {

  def this(sc: SparkContext) {
    this(new SQLContext(sc).sparkSession)
  }

  def generateReport(request: ReportGenerationRequest, inputPaths: java.util.List[String], outputPath: String): Unit = {
    //def run(): Unit = {
    import spark.implicits._
    val aggRow = new RecordAgg().toColumn.alias("record").as[Record]
    val df = spark.read.format("com.databricks.spark.avro").load(asScalaBuffer(inputPaths): _*)
    //    val cols = Set(request.getFields)
    //    val filters = request.getFilters
    //    val sort = request.getSort

    // TODO: configure partitions. The default number of partitions is 200
    val aggDf = df.groupBy("program", "run").agg(aggRow).drop("program").drop("run")
    //    val request = ReportGen.GSON.fromJson("{\"start\":1520808000,\"end\":1520808005}", classOf[Request])
    val start: Long = request.getStart
    val end: Long = request.getEnd
    val filteredDf = aggDf.filter(aggDf("record").getField("start") < end && (aggDf("record").getField("end").isNull || (aggDf("record").getField("end").isNotNull && aggDf("record").getField("end") > start))).persist()
    val records = filteredDf.select("record").rdd.mapPartitions(rIter => new Iterator[org.apache.spark.sql.Row] {
      override def hasNext: Boolean = rIter.hasNext

      override def next(): org.apache.spark.sql.Row = rIter.next.getAs[org.apache.spark.sql.Row]("record")
    }, preservesPartitioning = true)
    spark.createDataFrame(records, records.first.schema).coalesce(1).write.json(outputPath)
  }

  def writeReport(ds: Dataset[Row]): Unit = {
    //    val filteredDf = aggDf.filter(aggDf("record").getField("start") < end && (aggDf("record").getField("end").isNull || (aggDf("record").getField("end").isNotNull && aggDf("record").getField("end") > start))).persist()
    //    // Use mapPartitions to avoid unnecessary shuffling
    val records = ds.select("record").rdd.mapPartitions(rIter => new Iterator[org.apache.spark.sql.Row] {
      override def hasNext: Boolean = rIter.hasNext;

      override def next(): org.apache.spark.sql.Row = rIter.next.getAs[org.apache.spark.sql.Row]("record")
    }, true)
    spark.createDataFrame(records, records.first.schema).coalesce(1).write.json("/Users/Chengfeng/tmp/report-id/")
    //    val count = filteredDf.count()
    //    // FileWriter
    //    val file = new File("/Users/Chengfeng/tmp/report-id/COUNT")
    //    var bw: Option[BufferedWriter] = None
    //    try {
    //      bw = Some(new BufferedWriter(new FileWriter(file)))
    //      bw.get.write("%d\n".format(count))
    //    } finally {
    //      bw.foreach(w => w.close())
    //    }
  }
}

case class Request(start: Long, end: Long)

object ReportGen {

  import com.google.gson._

  val GSON = new Gson()
}
