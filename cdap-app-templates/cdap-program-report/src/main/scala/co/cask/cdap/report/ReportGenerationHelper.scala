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

import java.io.{IOException, PrintWriter}
import java.util.stream.Collectors

import co.cask.cdap.report.proto.Sort.Order
import co.cask.cdap.report.proto.{Sort, _}
import co.cask.cdap.report.util.Constants
import com.databricks.spark._
import com.google.gson._
import org.apache.avro.mapred._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.twill.filesystem.Location
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * A helper class for report generation.
  */
object ReportGenerationHelper {

  val GSON = new Gson()
  val LOG = LoggerFactory.getLogger(ReportGenerationHelper.getClass)
  val RECORD_COL = "record"
  val REQUIRED_FIELDS = Set(Constants.PROGRAM)
  val REQUIRED_FILTER_FIELDS = Set(Constants.START, Constants.END)
  val AVRO_READER = avro.AvroDataFrameReader(_)
  val FS_INPUT = classOf[FsInput]

  /**
    * Generates a report file according to the given request from the given program run meta files.
    * The given program run meta files are first read into a single [[org.apache.spark.sql.DataFrame]].
    * The [[org.apache.spark.sql.DataFrame]] is then grouped by program run ID and aggregated to form
    * a new aggregated [[org.apache.spark.sql.DataFrame]] with a column "run" containing program run ID and a column
    * "record" containing [[Record]] objects as shown below:
    * +---------+----------+
    * |   run   |  record  |
    * +---------+----------+
    * The request is then used to obtain names of the fields in [[Record]] to be included
    * in the final report and the fields that are used for filtering or sorting. New columns containing
    * those fields will be added to the aggregated [[org.apache.spark.sql.DataFrame]] as shown below:
    * +---------+----------+-----------------------------------------------------
    * |   run   |  record  |  required columns, filter columns, sort columns ...
    * +---------+----------+-----------------------------------------------------
    * For instance, if the required columns, filter columns and sort columns combined only contain three columns
    * "namespace", "program", and "duration", the aggregated [[org.apache.spark.sql.DataFrame]]
    * will contain columns as shown below:
    * +---------+----------+---------------+-----------+------------+
    * |   run   |  record  |   namespace   |  program  |  duration  |
    * +---------+----------+---------------+-----------+------------+
    * After filtering and sorting are done on the [[org.apache.spark.sql.DataFrame]],
    * only the columns required in the report will be kept in the [[org.apache.spark.sql.DataFrame]] as shown below:
    * +---------------------+--------------------+----------------------
    * |  required column 1  | required column 2  | required columns ...
    * +---------------------+--------------------+----------------------
    * For instance, if the required columns only contain three columns "namespace", "program", and "run",
    * the final [[org.apache.spark.sql.DataFrame]] will contain columns as shown below:
    * +---------+---------------+-----------+
    * |   run   |   namespace   |  program  |
    * +---------+---------------+-----------+
    * The final [[org.apache.spark.sql.DataFrame]] will be written to a JSON file at the given output location,
    * accompanied by an empty _SUCCESS file indicating success.
    *
    * @param spark the spark session to run report generation with
    * @param request the report generation request
    * @param inputURIs URIs of the avro files containing program run meta records
    * @param reportIdDir location of the directory where the report files directory, COUNT file,
    *                      and _SUCCESS file will be created.
    * @throws java.io.IOException when fails to write to the COUNT or _SUCCESS file
    */
  @throws(classOf[IOException])
  def generateReport(spark: SparkSession, request: ReportGenerationRequest, inputURIs: java.util.List[String],
                     reportIdDir: Location): Unit = {
    import spark.implicits._
    val df = spark.read.format("com.databricks.spark.avro").load(inputURIs: _*)
    // Get the fields to be included in the final report and additional fields required for filtering and sorting
    val (reportFields: Set[String], additionalFields: Set[String]) = getReportAndAdditionalFields(request)
    // Create an aggregator that aggregates grouped data into a column with data type Record.
    val aggCol = new RecordAggregator().toColumn.alias(RECORD_COL).as[Record]
    // TODO: configure partitions. The default number of partitions is 200
    // Group the program run meta records by program run Id's and aggregate the grouped data with aggCol.
    // The initial aggregated DataFrame will have two columns:
    // With every unique field in reportFields and additionalFields, construct and add new columns from record column
    // in aggregated DataFrame, in addition to the two initial columns "run" and "record"
    val aggDf = (reportFields ++ additionalFields).foldLeft(df.groupBy(Constants.RUN).agg(aggCol))((df, fieldName) =>
      df.withColumn(fieldName, df(RECORD_COL).getField(fieldName)))
    // Filter the aggregated DataFrame
    var resultDf = aggDf.filter(getFilter(request, aggDf))
    // If sort is specified in the request, apply sorting to the result DataFrame
    Option(request.getSort).foreach(_.foreach(sort => {
      val sortField = aggDf(sort.getFieldName)
      sort.getOrder match {
        case Order.ASCENDING => {
          resultDf = resultDf.sort(sortField.asc)
          LOG.debug("Sort by {} in ascending order", sortField)
        }
        case Sort.Order.DESCENDING => {
          resultDf = resultDf.sort(sortField.desc)
          LOG.debug("Sort by {} in descending order", sortField)
        }
      }
    }))
    // drop the columns which should not be included in the report
    resultDf.columns.foreach(col => if (!reportFields.contains(col)) resultDf = resultDf.drop(col))
    resultDf.persist()
    // Writing the DataFrame to JSON files requires a non-existing directory to write report files.
    // Create a non-existing directory location with name ReportSparkHandler.REPORT_DIR
    val reportDir = reportIdDir.append(Constants.LocationName.REPORT_DIR).toURI.toString
    // TODO: [CDAP-13290] output reports as avro instead of json text files
    // TODO: [CDAP-13291] improve how the number of partitions is configured
    resultDf.coalesce(1).write.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(reportDir)
    val count = resultDf.count
    // Write the total number of records in _SUCCESS file generated after successful report generation
    var writer: Option[PrintWriter] = None
    try {
      val countFile = reportIdDir.append(Constants.LocationName.COUNT_FILE)
      countFile.createNew
      writer = Some(new PrintWriter(countFile.getOutputStream))
      writer.get.write(count.toString)
    } catch {
      case e: IOException => {
        LOG.error("Failed to write to {} in {}", Constants.LocationName.COUNT_FILE, reportIdDir.toURI.toString, e)
        throw e
      }
    } finally if (writer.isDefined) writer.get.close()
    reportIdDir.append(Constants.LocationName.SUCCESS_FILE).createNew()
  }

  /**
    * Gets the fields to be included in the final report and additional fields required for filtering and sorting
    *
    * @param request the report generation request
    * @return a tuple containing the set of fields to be included in the final report and
    *         the set of additional fields for filtering and sorting
    */
  def getReportAndAdditionalFields(request: ReportGenerationRequest): (Set[String], Set[String]) = {
    // Construct a set of fields to be included in the final report with required fields and fields from the request
    val reportFields: Set[String] = REQUIRED_FIELDS ++ Option(request.getFields).map(_.toSet).getOrElse(Nil)
    LOG.debug("Fields to be included in the report: {}", reportFields)
    // Initialize the set with "start" and "end" for filtering records according to the time range [start, end)
    // specified in the request.
    val additionalFields: Set[String] = REQUIRED_FILTER_FIELDS ++
      // Add field names for filtering
      Option(request.getFilters).map(_.toSet[Filter[_]].map(_.getFieldName)).getOrElse(Nil) ++
      // Add field names for sorting
      Option(request.getSort).map(_.toSet[Sort].map(_.getFieldName)).getOrElse(Nil)
    LOG.debug("Additional fields for filtering and sorting: {}", additionalFields)
    (reportFields, additionalFields)
  }

  /**
    * Gets a filter constructed from the report time range and filters in the report generation request.
    *
    * @param request the report generation request
    * @param df the DateFrame to apply filter on
    * @return the filter
    */
  def getFilter(request: ReportGenerationRequest, df: DataFrame): Column = {
    // Construct the filter column starting with condition:
    // aggDf("start") not null AND aggDf("start") < request.getEnd
    //   AND (aggDf("end") is null OR aggDf("end") >= request.getStart)
    // Then combine additional filters from the request with AND
    val filterCol = Option(request.getFilters).map(_.toList).getOrElse(Nil).foldLeft(
      df(Constants.START).isNotNull && df(Constants.START) < request.getEnd &&
        (df(Constants.END).isNull || df(Constants.END) >= request.getStart))(
      (fCol: Column, filter: Filter[_]) => {
        val fieldCol = df(filter.getFieldName)
        // the filed to be filtered must contain non-null value
        var newFilterCol = fieldCol.isNotNull
        // the filter is either a RangeFilter or ValueFilter. Construct the filter according to the filter type
        filter match {
          case rangeFilter: RangeFilter[_] => {
            val min = rangeFilter.getRange.getMin
            if (Option(min).isDefined) {
              newFilterCol &&= fieldCol >= min
            }
            val max = rangeFilter.getRange.getMax
            if (Option(max).isDefined) {
              newFilterCol &&= fieldCol < max
            }
            // cast filter.getFieldName to Any to avoid ambiguous method reference error
            LOG.debug("Added RangeFilter {} for field {}", rangeFilter, filter.getFieldName: Any)
          }
          case valueFilter: ValueFilter[_] => {
            val whitelist = valueFilter.getWhitelist
            newFilterCol &&= fieldCol.isin(whitelist.stream().collect(Collectors.toList()): _*)
            val blacklist = valueFilter.getBlacklist
            newFilterCol &&= !fieldCol.isin(blacklist.stream().collect(Collectors.toList()): _*)
            // cast filter.getFieldName to Any to avoid ambiguous method reference error
            LOG.debug("Added ValueFilter {} for field {}", valueFilter, filter.getFieldName: Any)
          }
        }
        fCol && newFilterCol
      })
    LOG.debug("Final filter column: {}", filterCol)
    filterCol
  }
}
