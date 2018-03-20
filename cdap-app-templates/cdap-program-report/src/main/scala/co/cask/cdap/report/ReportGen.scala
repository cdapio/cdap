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

import com.google.common.collect.ImmutableList

import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.slf4j.LoggerFactory

class ReportGen(private val spark: SparkSession) {

  def this(sc: SparkContext) {
    this(new SQLContext(sc).sparkSession)
  }

  def generateReport(request: ReportGenerationRequest, inputPaths: java.util.List[String], outputPath: String): Long = {
    //def run(): Unit = {
    import spark.implicits._
    import ReportGen._
    val aggRow = new RecordAgg().toColumn.alias(RECORD).as[Record]
    val df = spark.read.format("com.databricks.spark.avro").load(asScalaBuffer(inputPaths): _*)
    //    val cols = Set(request.getFields)
    //    val filters = request.getFilters
    //    val sort = request.getSort

    // TODO: configure partitions. The default number of partitions is 200
    val aggDf = df.groupBy(ReportField.RUN.getName).agg(aggRow).drop("program").drop("run")
    //    val request = ReportGen.GSON.fromJson("{\"start\":1520808000,\"end\":1520808005}", classOf[Request])
    val start: Long = request.getStart
    val end: Long = request.getEnd
    val recordCol = aggDf(RECORD)
    var filterCol = recordCol.getField("start").isNotNull
    filterCol &&= recordCol.getField("start") < end
    filterCol &&= (recordCol.getField("end").isNull
      || recordCol.getField("end") > start)
    LOG.info("filterCol={}", filterCol)
    if (Option(request.getFilters).isDefined) {
      asScalaBuffer(request.getFilters).foreach(f => {
        val fieldCol = recordCol.getField(f.getFieldName)
        filterCol &&= fieldCol.isNotNull
        f match {
          case rangeFilter: ReportGenerationRequest.RangeFilter[_] => {
            LOG.info("rangeFilter for field {}", f.getFieldName)
            val min = rangeFilter.getRange.getMin
            if (Option(min).isDefined) filterCol &&= fieldCol >= min
            val max = rangeFilter.getRange.getMax
            if (Option(max).isDefined) filterCol &&= fieldCol < max
          }
          case valueFilter: ReportGenerationRequest.ValueFilter[String] => {
            LOG.info("valueFilter for field {}", f.getFieldName)
            val whitelist: java.util.List[String] = valueFilter.getWhitelist
            if (Option(whitelist).isDefined) {
              LOG.info("whitelist = {}", whitelist)
              filterCol &&= fieldCol.isin(asScalaBuffer(whitelist):_*)
            }
            val blacklist = valueFilter.getBlacklist
            if (Option(blacklist).isDefined) filterCol &&= !fieldCol.isin(asScalaBuffer(blacklist):_*)
          }
        }
      })
    }
   LOG.info("filterCol={}", filterCol)
    var resultDf = aggDf.filter(filterCol).persist()
    if (Option(request.getSort).isDefined) {
      asScalaBuffer(request.getSort).foreach(sort => {
        val sortField = recordCol.getField(sort.getFieldName)
        sort.getOrder match {
          case ReportGenerationRequest.Order.ASCENDING => {resultDf = resultDf.sort(sortField.asc)}
          case ReportGenerationRequest.Order.DESCENDING => {resultDf = resultDf.sort(sortField.desc)}
        }
      })
    }
    val records = resultDf.select(RECORD).rdd.mapPartitions(rIter => new Iterator[org.apache.spark.sql.Row] {
      override def hasNext: Boolean = rIter.hasNext
      override def next(): org.apache.spark.sql.Row = rIter.next.getAs[org.apache.spark.sql.Row](RECORD)
    }, preservesPartitioning = true).persist()
//    if (!records.isEmpty()) {
      spark.createDataFrame(records, records.first.schema).coalesce(1).write.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(outputPath)
//    }
    records.count
  }
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

object ReportGen {

  import com.google.gson._

  val GSON = new Gson()
  val LOG = LoggerFactory.getLogger(ReportGen.getClass)
  val RECORD = "record"
}
