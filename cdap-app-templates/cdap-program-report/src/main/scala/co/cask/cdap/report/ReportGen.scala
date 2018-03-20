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
import org.apache.twill.filesystem.Location
import org.slf4j.LoggerFactory

class ReportGen(private val spark: SparkSession) {

  def this(sc: SparkContext) {
    this(new SQLContext(sc).sparkSession)
  }

  def generateReport(request: ReportGenerationRequest, inputPaths: java.util.List[String], outputPath: String): Long = {
    //def run(): Unit = {
    import spark.implicits._
    import ReportGen._
    val aggRow = new RecordAgg().toColumn.alias(RECORD_COL).as[Record]
    val df = spark.read.format("com.databricks.spark.avro").load(asScalaBuffer(inputPaths): _*)
    //    val cols = Set(request.getFields)
    //    val filters = request.getFilters
    //    val sort = request.getSort

    // TODO: configure partitions. The default number of partitions is 200
    var aggDf = df.groupBy(ReportField.RUN.getFieldName).agg(aggRow)
    //    val request = ReportGen.GSON.fromJson("{\"start\":1520808000,\"end\":1520808005}", classOf[Request])
    val start: Long = request.getStart
    val end: Long = request.getEnd
    val recordCol = aggDf(RECORD_COL)
    val requiredFields = collection.mutable.LinkedHashSet(ReportField.NAMESPACE.fieldName, ReportField.PROGRAM.fieldName, ReportField.RUN.fieldName)
    if (Option(request.getFields).isDefined) requiredFields ++= request.getFields
    LOG.info("requiredFields= {}", requiredFields)
    val additionalFields = collection.mutable.LinkedHashSet(ReportField.START.fieldName, ReportField.END.fieldName)
    LOG.info("additionalFields= {}", additionalFields)
    if (Option(request.getFilters).isDefined) asScalaBuffer(request.getFilters).foreach(f => additionalFields.add(f.getFieldName))
    if (Option(request.getSort).isDefined) asScalaBuffer(request.getSort).foreach(s => additionalFields.add(s.getFieldName))
    (requiredFields ++ additionalFields).foreach(fieldName => {
      LOG.info("adding column {}", fieldName)
      aggDf = aggDf.withColumn(fieldName, recordCol.getField(fieldName))
      LOG.info("aggDf.columns={}", aggDf.columns)
    })
    var filterCol = aggDf(ReportField.START.fieldName).isNotNull
    filterCol &&= aggDf(ReportField.START.fieldName) < end
    filterCol &&= (aggDf(ReportField.END.fieldName).isNull
      || aggDf(ReportField.END.fieldName) > start)
    LOG.info("filterCol={}", filterCol)
    if (Option(request.getFilters).isDefined) {
      asScalaBuffer(request.getFilters).foreach(f => {
        val fieldCol = aggDf(f.getFieldName)
        filterCol &&= fieldCol.isNotNull
        f match {
          case rangeFilter: ReportGenerationRequest.RangeFilter[_] => {
            LOG.info("rangeFilter for field {}", f.getFieldName)
            val min = rangeFilter.getRange.getMin
            if (Option(min).isDefined) filterCol &&= fieldCol >= min
            val max = rangeFilter.getRange.getMax
            if (Option(max).isDefined) filterCol &&= fieldCol < max
          }
          case valueFilter: ReportGenerationRequest.ValueFilter[_] => {
            LOG.info("valueFilter for field {}", f.getFieldName)
            val whitelist = valueFilter.getWhitelist
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
    var resultDf = aggDf.filter(filterCol)
    if (Option(request.getSort).isDefined) {
      asScalaBuffer(request.getSort).foreach(sort => {
        val sortField = aggDf(sort.getFieldName)
        sort.getOrder match {
          case ReportGenerationRequest.Order.ASCENDING => {resultDf = resultDf.sort(sortField.asc)}
          case ReportGenerationRequest.Order.DESCENDING => {resultDf = resultDf.sort(sortField.desc)}
        }
      })
    }
    resultDf.columns.foreach(col => if (!requiredFields.contains(col)) resultDf = resultDf.drop(col))
    resultDf.persist()
    resultDf.coalesce(1).write.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(outputPath)
    resultDf.count
  }
}

object ReportGen {

  import com.google.gson._

  val GSON = new Gson()
  val LOG = LoggerFactory.getLogger(ReportGen.getClass)
  val RECORD_COL = "record"
}
