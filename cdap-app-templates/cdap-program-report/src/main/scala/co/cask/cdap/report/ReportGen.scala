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

import co.cask.cdap.report.proto.ReportGenerationRequest
import co.cask.cdap.report.util.ReportField
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class ReportGen(private val spark: SparkSession) {

  def this(sc: SparkContext) {
    this(new SQLContext(sc).sparkSession)
  }

  def generateReport(request: ReportGenerationRequest, inputPaths: java.util.List[String], outputPath: String): Long = {
    import ReportGen._
    import spark.implicits._
    val df = spark.read.format("com.databricks.spark.avro").load(asScalaBuffer(inputPaths): _*)
    // Group the program run meta records by program run Id's and aggregate grouped records into a column
    // with data type Record. The aggregated DataFrame aggDf will have two columns: "run" and "record"
    val aggCol = new RecordAgg().toColumn.alias(RECORD_COL).as[Record]
    // TODO: configure partitions. The default number of partitions is 200
    var aggDf = df.groupBy(ReportField.RUN.getFieldName).agg(aggCol)
    // Construct a set of fields to be included in the final report with required fields and fields from the request
    val reportFields = collection.mutable.LinkedHashSet(REQUIRED_FIELDS: _*)
    if (Option(request.getFields).isDefined) reportFields ++= request.getFields
    LOG.debug("reportFields={}", reportFields)
    // Construct a set of additional fields to be included as columns in the aggregated DataFrame
    // with fields used for filtering and sorting
    val additionalFields = collection.mutable.LinkedHashSet(ReportField.START.fieldName, ReportField.END.fieldName)
    if (Option(request.getFilters).isDefined) {
      asScalaBuffer(request.getFilters).foreach(f => additionalFields.add(f.getFieldName))
    }
    if (Option(request.getSort).isDefined) {
      asScalaBuffer(request.getSort).foreach(s => additionalFields.add(s.getFieldName))
    }
    LOG.debug("additionalFields={}", additionalFields)
    // With every unique field in reportFields and additionalFields, construct and add new columns from record column
    // in aggregated DataFrame
    (reportFields ++ additionalFields).foreach(fieldName => {
      aggDf = aggDf.withColumn(fieldName, aggDf(RECORD_COL).getField(fieldName))
    })
    // Construct the filter column starting with condition:
    // aggDf("start") not null AND aggDf("start") < request.getEnd
    //   AND (aggDf("end") is null OR aggDf("end") > request.getStart)
    var filterCol = aggDf(ReportField.START.fieldName).isNotNull && aggDf(ReportField.START.fieldName) < request.getEnd
    && (aggDf(ReportField.END.fieldName).isNull || aggDf(ReportField.END.fieldName) > request.getStart)
    LOG.info("initial filterCol={}", filterCol)
    // Combine additional filters from the request to the filter column
    if (Option(request.getFilters).isDefined) {
      asScalaBuffer(request.getFilters).foreach(f => {
        val fieldCol = aggDf(f.getFieldName)
        // the filed to be filtered must contain non-null value
        filterCol &&= fieldCol.isNotNull
        // the filter is either a RangeFilter or ValueFilter. Construct the filter according to the filter type
        f match {
          case rangeFilter: ReportGenerationRequest.RangeFilter[_] => {
            LOG.debug("rangeFilter for field {}", f.getFieldName)
            val min = rangeFilter.getRange.getMin
            if (Option(min).isDefined) {
              filterCol &&= fieldCol >= min
            }
            val max = rangeFilter.getRange.getMax
            if (Option(max).isDefined) {
              filterCol &&= fieldCol < max
            }
          }
          case valueFilter: ReportGenerationRequest.ValueFilter[_] => {
            LOG.debug("valueFilter for field {}", f.getFieldName)
            val whitelist = valueFilter.getWhitelist
            if (Option(whitelist).isDefined) {
              filterCol &&= fieldCol.isin(asScalaBuffer(whitelist): _*)
            }
            val blacklist = valueFilter.getBlacklist
            if (Option(blacklist).isDefined) {
              filterCol &&= !fieldCol.isin(asScalaBuffer(blacklist): _*)
            }
          }
        }
      })
    }
    LOG.info("final filterCol={}", filterCol)
    var resultDf = aggDf.filter(filterCol)
    // If sort is specified in the request, apply sorting to the result DataFrame
    if (Option(request.getSort).isDefined) {
      asScalaBuffer(request.getSort).foreach(sort => {
        val sortField = aggDf(sort.getFieldName)
        sort.getOrder match {
          case ReportGenerationRequest.Order.ASCENDING => {
            resultDf = resultDf.sort(sortField.asc)
          }
          case ReportGenerationRequest.Order.DESCENDING => {
            resultDf = resultDf.sort(sortField.desc)
          }
        }
      })
    }
    // drop the columns which should not be included in the report
    resultDf.columns.foreach(col => if (!reportFields.contains(col)) resultDf = resultDf.drop(col))
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
  val REQUIRED_FIELDS = Seq(ReportField.NAMESPACE.fieldName, ReportField.PROGRAM.fieldName, ReportField.RUN.fieldName)
}
