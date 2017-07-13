/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.spark.sql.datasources.stream

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.data.format.{FormatSpecification, StructuredRecord}
import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.spark.sql.DataFrames
import co.cask.cdap.api.stream.GenericStreamEventData
import co.cask.cdap.app.runtime.spark.SparkClassLoader
import co.cask.cdap.format.RecordFormats
import co.cask.cdap.proto.id.StreamId
import com.google.common.annotations.VisibleForTesting
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

/**
  * A BaseRelation and PrunedFilteredScan for reading from Stream as DataFrame.
  *
  * By default the stream schema is based on the stream [[co.cask.cdap.api.data.format.FormatSpecification]].
  * It can be overridden by the `stream.format` parameter with an optional schema provided through the DataFrame API
  * with all parameters provided to this class will be pass to the [[co.cask.cdap.api.data.format.FormatSpecification]]
  * as well.
  * If the `stream.format` is set to `raw`, then the stream body will always be returned as binary data type.
  *
  * The timestamp and headers columns will be added automatically to the schema, unless turned off by setting
  * `timestamp.column.enabled` or `headers.column.enabled` to `false`. The name of the timestamp and headers column
  * are defaulted to `timestamp` and `headers` respectively, but can be overridden by the parameters
  * `timestamp.column.name` and `headers.column.name`.
  */
private[stream] class StreamRelation(override val sqlContext: SQLContext, streamId: StreamId,
                                     userSchema: Option[StructType], parameters: Map[String, String])
  extends BaseRelation with Serializable with PrunedFilteredScan {

  import StreamRelation._

  private val timestampColName = determineColumnName(TIMESTAMP_COL_ENABLED, TIMESTAMP_COL_NAME, "ts")
  private val headersColName = determineColumnName(HEADERS_COL_ENABLED, HEADERS_COL_NAME, "headers")

  // The stream format spec Option based on the user parameters and stream setting.
  // If the user choose to use "raw" format, this will be None
  private lazy val formatSpec = createStreamFormat()

  // The body schema is either whatever user provided, or from the format spec or raw
  private lazy val streamBodySchema =
    formatSpec
      .map(f => userSchema.getOrElse(DataFrames.toDataType[StructType](f.getSchema)))
      .getOrElse(StructType(Seq(StructField("body", DataTypes.BinaryType, false))))

  // The actual schema is the body schema with optionally timestamp and headers columns added
  private lazy val streamSchema = addTimestampAndHeaders(streamBodySchema)

  override def schema: StructType = {
    streamSchema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // If there is no filter or if there is no timestamp column, we can't filter by timestamp, hence
    // need to do a full scan
    if (filters.length == 0 || timestampColName.isEmpty) {
      scanStream(requiredColumns, 0, Long.MaxValue)
    } else {
      // Determine the timestamp ranges based on the filters. Only take ranges with startTime < endTime
      determineTimeRanges(timestampColName.get, filters)
          .filter(range => range._1 < range._2)
          .foldLeft(sqlContext.sparkContext.emptyRDD : RDD[Row])((rdd, range) => {
            // union all rdds from each time range
            rdd ++ scanStream(requiredColumns, range._1, range._2)
          })
    }
  }

  /**
    * Determine the optional column name.
    *
    * @param enableKey key in the parameters to determine if an optional column is enabled
    * @param nameKey key in the parameters to determine the column name if the column is enabled
    * @param defaultName the default column name if the column is enabled but doesn't have a column name
    *                    provided in the parameters
    * @return an [[scala.Option]] of [[String]]. The option will be [[scala.None]] if the column should be disabled.
    */
  private def determineColumnName(enableKey: String, nameKey : String, defaultName: String) : Option[String] = {
    if (Try(parameters.getOrElse(enableKey, "true").toBoolean).getOrElse(true)) {
      parameters.get(nameKey).orElse(Some(defaultName))
    } else {
      None
    }
  }

  /**
    * Scans the stream from the given time range. The stream events will be converted to Rows, based on the
    * format specification.
    *
    * @param requiredColumns the list of columns needed in the resulting Row
    * @param startTime start time for the scan (inclusive)
    * @param endTime end time for the scan (exclusive)
    * @return a RDD of Row
    */
  private def scanStream(requiredColumns: Array[String], startTime: Long, endTime: Long): RDD[Row] = {
    val sec = SparkClassLoader.findFromContext().getSparkExecutionContext(false)
    val sc = sqlContext.sparkContext

    formatSpec.fold({
      // No format spec, use raw converter
      implicit val converter = createStreamEventConverter(timestampColName, headersColName, requiredColumns)
      sec.fromStream[Row](sc, streamId.getNamespace, streamId.getStream, startTime, endTime)
    })(format => {
      // Use format spec
      val converter = createStructuredRecordConverter(timestampColName, headersColName,
                                                      requiredColumns, streamBodySchema)
      sec.fromStream[StructuredRecord](sc, streamId.getNamespace, streamId.getStream, format, startTime, endTime)
         .map(converter)
    })
  }

  /**
    * Creates a [[co.cask.cdap.api.data.format.FormatSpecification]] based on the stream configuration and
    * and optional user provided schema.
    */
  private def createStreamFormat(): Option[FormatSpecification] = {
    // If the user specifically want the raw format, don't use FormatSpecification
    if (parameters.get(STREAM_FORMAT).exists(_ == "raw")) {
      return None
    }

    // See if the user provide a stream format from the parameters
    // If it does, create a format spec from it
    val providedSchema = userSchema.map(DataFrames.toSchema)
    val providedFormatSpec = parameters.get(STREAM_FORMAT).map(format => {
      val schema = RecordFormats.createInitializedFormat(
        new FormatSpecification(format, providedSchema.orNull, parameters)).getSchema
      new FormatSpecification(format, schema, parameters)
    })

    // Either return the format spec created above based on the user provided format + schema
    // Otherwise, return the one set on the stream, optionally overridding the schema with the user provided one
    Some(providedFormatSpec.getOrElse({
      val runtimeContext = SparkClassLoader.findFromContext().getRuntimeContext
      val defaultFormatspec = runtimeContext.getStreamAdmin.getConfig(streamId).getFormat
      new FormatSpecification(
        defaultFormatspec.getName,
        providedSchema.getOrElse(RecordFormats.createInitializedFormat(defaultFormatspec).getSchema),
        defaultFormatspec.getSettings ++ parameters
      )
    }))
  }

  /**
    * Creates a new [[org.apache.spark.sql.types.StructType]] by optionally adding timestamp and headers
    * columns based on parameters, followed by all the fields from the given schema.
    */
  private def addTimestampAndHeaders(schema: StructType): StructType = {
    val fields = new mutable.ArrayBuffer[StructField]()

    // Optionally add timestamp and headers column.
    timestampColName.foreach(name => {
      if (schema.fieldNames.contains(name)) {
        throw new IllegalArgumentException(
          s"Cannot add timestamp field because the schema already has a '$name' field. " +
            s"Change the timestamp column name by setting the option '$TIMESTAMP_COL_NAME' or " +
            s"disable the timestamp column by setting '$HEADERS_COL_ENABLED' to 'false'")
      }
      fields += StructField(name, DataTypes.LongType, false)
    })
    headersColName.foreach(name => {
      if (schema.fieldNames.contains(name)) {
        throw new IllegalArgumentException(
          s"Cannot add headers field because the schema already has a '$name' field. " +
            s"Change the headers column name by setting the option '$HEADERS_COL_NAME' or " +
            s"disable the headers column by setting '$HEADERS_COL_ENABLED' to 'false'")
      }
      fields += StructField(name, MapType(DataTypes.StringType, DataTypes.StringType, false), false)
    })

    // Copy all the fields from the existing schema as well
    StructType(fields ++ schema.fields)
  }
}

/**
  * Companion object to provide helper methods.
  */
private[stream] object StreamRelation {

  private val STREAM_FORMAT = "stream.format"
  private val TIMESTAMP_COL_ENABLED = "timestamp.column.enabled"
  private val HEADERS_COL_ENABLED = "headers.column.enabled"
  private val TIMESTAMP_COL_NAME = "timestamp.column.name"
  private val HEADERS_COL_NAME = "headers.column.name"

  /**
    * Determine a list of time ranges for the scan.
    *
    * @param colName column name of the timestamp column
    * @param filters the set of filters from the SQL expression, provided by Spark.
    *                By Spark definition, they will be AND together
    * @return A sequence of tuple with (startTime, endTime) for scanning from stream
    */
  @VisibleForTesting
  def determineTimeRanges(colName: String, filters: Array[Filter]): Seq[(Long, Long)] = {
    sortAndCombineRanges(filters.map(determineTimeRanges(colName, _, 0, Long.MaxValue)).reduce(intersectRanges))
  }

  /**
    * Determine a list of time ranges based on the given Filter.
    *
    * @param colName column name of the timestamp column
    * @param filter the filter to use
    * @param startTime lower bound of start time
    * @param endTime upper bound of end time
    * @param negate whether the filter result should be negated (based on usage of NOT filter in parent)
    * @return a sequence of (startTime, endTime) tuples
    */
  private def determineTimeRanges(colName: String, filter: Filter, startTime: Long, endTime: Long)
                                 (implicit negate: Boolean = false): Seq[(Long, Long)] = {
    determineFilter(filter) match {
      case EqualTo(colName, value: Long) => Seq(range(value, safeIncrement(value)))
      case EqualNullSafe(colName, value: Long) if value != null => Seq(range(value, safeIncrement(value)))
      case GreaterThan(colName, value: Long) => Seq(range(safeIncrement(value), endTime))
      case GreaterThanOrEqual(colName, value: Long) => Seq(range(value, endTime))
      case LessThan(colName, value: Long) => Seq(range(startTime, value))
      case LessThanOrEqual(colName, value: Long) => Seq(range(startTime, safeIncrement(value)))

      // Compute intersection of ranges
      case And(left, right) => intersectRanges(determineTimeRanges(colName, left, startTime, endTime),
                                               determineTimeRanges(colName, right, startTime, endTime))
      // Union left and right
      case Or(left, right) => sortAndCombineRanges(determineTimeRanges(colName, left, startTime, endTime) ++
                                                   determineTimeRanges(colName, right, startTime, endTime))

      // Create N ranges and combine
      case In(colName, values) => sortAndCombineRanges(values.distinct.map {
        case value: Long => range(value, safeIncrement(value))
      })

      // Flip the negation flag for child nodes
      case Not(child) => determineTimeRanges(colName, child, startTime, endTime)(!negate)
      case _ => Seq((startTime, endTime))
    }
  }

  /**
    * Determine the actual filter to use, based on the need for negation.
    *
    * @param filter the original filter
    * @param negate `true` to indicate negation is needed
    * @return
    */
  private def determineFilter(filter: Filter)(implicit negate: Boolean): Filter = {
    negate match {
      case true => filter match {
        // For >, >=, <, <= cases, flip the direction
        case GreaterThan(attribute, value) => LessThanOrEqual(attribute, value)
        case GreaterThanOrEqual(attribute, value) => LessThan(attribute, value)
        case LessThan(attribute, value) => GreaterThanOrEqual(attribute, value)
        case LessThanOrEqual(attribute, value) => GreaterThan(attribute, value)

        // For = and IN, just treat it as no filter, as the benefits of having multiple large time ranges is neglectable
        case EqualTo(attribute, value) => null
        case EqualNullSafe(attribute, value) => null
        case In(attribute, values) => null

        // For OR, it becomes AND. For AND it becomes OR.
        // Negation will also apply to the child filters
        case Or(left, right) => And(left, right)
        case And(left, right) => Or(left, right)

        // For all other types, just keep it as is
        case _ => filter
      }
      case false => filter
    }
  }

  /**
    * Makes sure start and end are >= 0 and returns a tuple of (start, end),
    */
  private def range(start: Long, end: Long): (Long, Long) = {
    (Math.max(0L, start), Math.max(0L, end))
  }

  /**
    * Safely increment the given value by 1 without overflowing
    */
  private def safeIncrement(value: Long): Long = {
    if (value < Long.MaxValue) {
      value + 1
    } else {
      value
    }
  }

  /**
    * Sorts the given sequence of (startTime, endTime) tuples based on the start time. If there are overlapping
    * ranges, they will be merged to form a larger range.
    */
  private def sortAndCombineRanges(ranges: Seq[(Long, Long)]): Seq[(Long, Long)] = {
    ranges.sortBy(_._1).foldLeft(Seq[(Long, Long)]())((acc, range) => {
      acc match {
        // The next range is completely contained inside previous one, simply return the accumulated sequence
        case head :+ tail if tail._2 >= range._1 && tail._2 >= range._2 => acc
        // The next range is overlapped with previous one, extends the tail element range
        case head :+ tail if tail._2 >= range._1 => head :+ (tail._1, range._2)
        // No overlap, just append the range
        case _ => acc :+ range
      }
    })
  }

  /**
    * Computes the intersection between two sequences of time ranges and return those that has overlapping ranges
    * between them.
    */
  private def intersectRanges(left: Seq[(Long, Long)], right: Seq[(Long, Long)]): Seq[(Long, Long)] = {
    sortAndCombineRanges(left.foldLeft(Seq[(Long, Long)]())((acc, leftRange) => {
      // For each left range, find overlapping ranges from right
      acc ++ right.flatMap {
        case (start, end) => Seq((Math.max(leftRange._1, start), Math.min(leftRange._2, end))).filter(t => t._1 < t._2)
      }
    }))
  }

  /**
    * Creates a function convert [[co.cask.cdap.api.flow.flowlet.StreamEvent]] to Row.
    */
  private def createStreamEventConverter(timestampColName: Option[String],
                                         headersColName: Option[String],
                                         requiredColumns: Array[String]): (StreamEvent) => Row = {
    (event: StreamEvent) => {
      Row(requiredColumns.map {
        case ts if timestampColName.exists(_ == ts) => event.getTimestamp
        case headers if headersColName.exists(_ == headers) => event.getHeaders
        case _ => Bytes.toBytes(event.getBody)
      } : _*)
    }
  }

  /**
    * Creates a function to convert stream event to Row, based on the columns and schema.
    * This method is in the companion object to avoid complication in serialization.
    */
  private def createStructuredRecordConverter(
                     timestampColName: Option[String],
                     headersColName: Option[String],
                     requiredColumns: Array[String],
                     scanBodySchema: StructType): ((Long, GenericStreamEventData[StructuredRecord])) => Row = {
    (t: (Long, GenericStreamEventData[StructuredRecord])) => {
      val bodyRow = DataFrames.toRow(t._2.getBody, scanBodySchema)
      Row(requiredColumns.map {
        case ts if timestampColName.exists(_ == ts) => t._1
        case headers if headersColName.exists(_ == headers) => t._2.getHeaders
        case col => bodyRow.get(scanBodySchema.fieldIndex(col))
      } : _*)
    }
  }
}
