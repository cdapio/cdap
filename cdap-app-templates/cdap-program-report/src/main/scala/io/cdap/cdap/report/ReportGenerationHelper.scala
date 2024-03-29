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
package io.cdap.cdap.report

import java.io.{IOException, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.stream.Collectors
import io.cdap.cdap.report.proto.Sort.Order
import io.cdap.cdap.report.proto.summary._
import io.cdap.cdap.report.proto.{Sort, _}
import io.cdap.cdap.report.util.Constants
import com.google.gson._
import org.apache.avro.mapred._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, max, min}
import org.apache.twill.filesystem.Location
import org.slf4j.LoggerFactory

import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * A helper class for report generation.
  */
object ReportGenerationHelper {

  val GSON = new Gson()
  val LOG = LoggerFactory.getLogger(ReportGenerationHelper.getClass)
  val RECORD_COL = "record"
  val REQUIRED_FIELDS = Set(Constants.PROGRAM)
  val REQUIRED_FILTER_FIELDS = Set(Constants.START, Constants.END)
  val REQUIRED_SUMMARY_FIELDS = Set(Constants.NAMESPACE, Constants.ARTIFACT_NAME, Constants.ARTIFACT_VERSION,
    Constants.ARTIFACT_SCOPE, Constants.DURATION, Constants.START, Constants.USER, Constants.START_METHOD)
  val FS_INPUT = classOf[FsInput]
  // the default name of the column created by calling aggregate function count
  val COUNT_COL = "count"

  /**
    * Generates a report file according to the given request from the given program run meta files.
    * The given program run meta files are first read into a single [[org.apache.spark.sql.DataFrame]].
    * The [[org.apache.spark.sql.DataFrame]] is then grouped by program run ID and aggregated to form
    * a new aggregated [[org.apache.spark.sql.DataFrame]] with a column "run" containing program run ID and a column
    * "record" containing fields as shown below:
    * +---------+----------+
    * |   run   |  record  |
    * +---------+----------+
    * The request is then used to obtain names of the fields in a record to be included
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
    * @param sql the SQL context to run report generation with
    * @param request the report generation request
    * @param inputURIs URIs of the avro files containing program run meta records
    * @param reportIdDir location of the directory where the report files directory, COUNT file,
    *                    and _SUCCESS file will be created.
    * @throws java.io.IOException when fails to write to the COUNT or _SUCCESS file
    */
  @throws(classOf[IOException])
  def generateReport(sql: SQLContext, request: ReportGenerationRequest, inputURIs: java.util.List[String],
                     reportIdDir: Location, reportExpiryDurationMillis : Long): Unit = {
    if (inputURIs.isEmpty) {
      writeEmptySummaryFile(request, reportIdDir, reportExpiryDurationMillis);
      return
    }
    val df = SparkCompat.readAvroFiles(sql, inputURIs)
    // Get the fields to be included in the final report and additional fields required for filtering and sorting
    val (reportFields: Set[String], additionalFields: Set[String]) = getReportAndAdditionalFields(request)

    // TODO: configure partitions. The default number of partitions is 200
    // Group the program run meta records by program runId's and aggregate the grouped data to get an
    // aggregated DataFrame with two columns: column "run" with runId's and column "record" with aggregation results
    val initAggDf = SparkCompat.aggregate(sql, df)
    // With every unique field in reportFields and additionalFields, construct and add new columns from record column
    // in aggregated DataFrame, in addition to the two initial columns "run" and "record"
    val aggDf = (reportFields ++ additionalFields).foldLeft(initAggDf)((df, fieldName) =>
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
    resultDf.persist()
    val reportSummary = getSummary(request, resultDf, reportExpiryDurationMillis)
    // drop the columns which should not be included in the report
    resultDf.columns.foreach(col => if (!reportFields.contains(col)) resultDf = resultDf.drop(col))
    val reportDir = reportIdDir.append(Constants.LocationName.REPORT_DIR).toURI.toString
    // TODO: [CDAP-13291] improve how the number of partitions is configured
    // by default spart sql uses snappy compression codec, set it to uncompressed
    sql.setConf("spark.sql.avro.compression.codec", "uncompressed")
    resultDf.coalesce(1).write.option("timestampFormat",
      "yyyy/MM/dd HH:mm:ss ZZ").format("avro").save(reportDir)
    writeSummaryToFile(reportSummary, reportIdDir)
  }

  /**
    * From the filters in ReportGenerationRequest figure out namespaces if they are provided, get the start
    * and end time range of query and use default for all other fields of report summary and write to summary file
    * with count 0
    * @param request ReportGenerationRequest
    * @param reportIdDir report directory where the summary, count and success files will be written to
    */
  private def writeEmptySummaryFile(request: ReportGenerationRequest,
                                    reportIdDir: Location,
                                    reportExpiryDurationMillis : Long) = {
    val namespacesAggregates = getNamespaceAggregates(request)
    val count = 0
    val creationTime = System.currentTimeMillis();
    val summary = new ReportSummary(namespacesAggregates, request.getStart, request.getEnd,
      new ArrayBuffer[ArtifactAggregate](), new DurationStats(0L, 0L, 0.0),
      new StartStats(0l, 0l), new ArrayBuffer[UserAggregate](), new ArrayBuffer[StartMethodAggregate](),
      count, creationTime, reportExpiryDurationMillis)
    writeSummaryToFile(summary, reportIdDir)
  }

  private def getNamespaceAggregates(request: ReportGenerationRequest) : ArrayBuffer[NamespaceAggregate] = {
    val namespacesAggregates = ArrayBuffer[NamespaceAggregate]()
    val filters = request.getFilters
    for (filter <- filters) {
      if (filter.getFieldName.equals(Constants.NAMESPACE)) {
        filter match {
          case valueFilter: ValueFilter[_] => {
            val namespaces = valueFilter.getWhitelist
            for (namespace <- namespaces) {
              namespacesAggregates.add(new NamespaceAggregate(namespace.asInstanceOf[String], 0));
            }
          }
        }
        return namespacesAggregates
      }
    }
    return namespacesAggregates
  }

  /**
    * Generates a summary of the report with the information from the report generation request and the DataFrame
    * containing the report details, then writes the summary to the given location.
    *
    * @param request the report generation request
    * @param df the DataFrame containing report details
    * @return summary of the report
    */
  private def getSummary(request: ReportGenerationRequest, df: DataFrame,
                         expiryDurationMillis : Long): ReportSummary = {
    val namespaces = ArrayBuffer[NamespaceAggregate]()
    // group the report details by namespace, and then collect the count and the corresponding unique namespaces
    df.groupBy(Constants.NAMESPACE).count.collect.foreach(r => namespaces +=
      new NamespaceAggregate(r.getAs[String](Constants.NAMESPACE), r.getAs[Long](COUNT_COL)))
    // group the report details by artifact information including artifact name, version and scope,
    // and then collect the count and the corresponding unique artifact information
    if (namespaces.isEmpty) {
      // if no records are found matching the request,
      // we still need to add the namespace aggregates with namespaces from the request and 0 records total.
      namespaces.addAll(getNamespaceAggregates(request));
    }
    val artifacts = ArrayBuffer[ArtifactAggregate]()
    df.groupBy(Constants.ARTIFACT_NAME, Constants.ARTIFACT_VERSION, Constants.ARTIFACT_SCOPE).count.collect
      .foreach(r => artifacts += new ArtifactAggregate(r.getAs[String](Constants.ARTIFACT_NAME),
        r.getAs[String](Constants.ARTIFACT_VERSION), r.getAs[String](Constants.ARTIFACT_SCOPE),
        r.getAs[Long](COUNT_COL)))
    // aggregate the report details into a row with the min, max, and average of duration,
    // and with the min and max of start
    val aggRow = df.agg(min(df(Constants.DURATION)).as("minDuration"), max(df(Constants.DURATION)).as("maxDuration"),
      avg(df(Constants.DURATION)).as("avgDuration"), min(df(Constants.START)).as("minStart"),
      max(df(Constants.START)).as("maxStart")).first
    // get the min, max, and average of duration
    val durations = new DurationStats(aggRow.getAs[Long]("minDuration"),
      aggRow.getAs[Long]("maxDuration"), aggRow.getAs[Double]("avgDuration"))
    // get the min and max of start
    val starts = new StartStats(aggRow.getAs[Long]("minStart"), aggRow.getAs[Long]("maxStart"))
    // group the report details by the user who starts the program run, and then collect the count and
    // the corresponding unique users
    val owners = ArrayBuffer[UserAggregate]()
    df.groupBy(Constants.USER).count.collect
      .foreach(r => owners += new UserAggregate(r.getAs[String](Constants.USER), r.getAs[Long](COUNT_COL)))
    // group the report details by the start method of the program run, and then collect the count and
    // the corresponding unique start methods
    val startMethods = ArrayBuffer[StartMethodAggregate]()
    df.groupBy(Constants.START_METHOD).count.collect
      .foreach(r => startMethods +=
        new StartMethodAggregate(r.getAs[String](Constants.START_METHOD), r.getAs[Long](COUNT_COL)))
    // create the summary
    return new ReportSummary(namespaces, request.getStart, request.getEnd, artifacts,
      durations, starts, owners, startMethods, df.count(), System.currentTimeMillis(), expiryDurationMillis)
  }

  private def writeSummaryToFile(summary : ReportSummary, reportIdDir: Location): Unit = {
    // Save the report summary request in the _SUMMARY file in the given directory
    val summaryLocation = reportIdDir.append(Constants.LocationName.SUMMARY)
    val tmpLocation = summaryLocation.getTempFile(Constants.LocationName.SUMMARY)
    try {
      val writer = new PrintWriter(new OutputStreamWriter(tmpLocation.getOutputStream, StandardCharsets.UTF_8))
      try {
        GSON.toJson(summary, writer)
      } finally {
        writer.close()
      }

      tmpLocation.renameTo(summaryLocation)
    } finally {
      try {
        tmpLocation.delete()
      } catch {
        // Location.delete() won't throw exception if the location does not exist
        case e => LOG.warn("Failed to delete temporary summary location {}", tmpLocation)
      }
    }
  }

  /**
    * Gets the fields to be included in the final report and additional fields required for filtering and sorting
    *
    * @param request the report generation request
    * @return a tuple containing the set of fields to be included in the final report and
    *         the set of additional fields for filtering and sorting
    */
  private def getReportAndAdditionalFields(request: ReportGenerationRequest): (Set[String], Set[String]) = {
    // Construct a set of fields to be included in the final report with required fields and fields from the request
    val reportFields: Set[String] = REQUIRED_FIELDS ++ Option(request.getFields).map(_.toSet).getOrElse(Nil)
    LOG.debug("Fields to be included in the report: {}", reportFields)
    // Initialize the set with "start" and "end" for filtering records according to the time range [start, end)
    // specified in the request, and also fields requried for generating the summary
    val additionalFields: Set[String] = REQUIRED_FILTER_FIELDS ++ REQUIRED_SUMMARY_FIELDS ++
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
  private def getFilter(request: ReportGenerationRequest, df: DataFrame): Column = {
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
            val blacklist = valueFilter.getBlacklist
            // only either of whitelist or blacklist can be non empty,
            // and a value filter will have one of them non empty
            if (whitelist.size() > 0) {
              newFilterCol &&= fieldCol.isin(whitelist.stream().collect(Collectors.toList()): _*)
            } else if (blacklist.size() > 0) {
              newFilterCol &&= !fieldCol.isin(blacklist.stream().collect(Collectors.toList()): _*)
            }
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
