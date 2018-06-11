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

import java.util.concurrent.TimeUnit

import co.cask.cdap.report.util.{Constants, ProgramStartMethodHelper}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
  * An aggregation function that aggregates [[Row]]'s with schema [[inputSchema]] belonging to the same group
  * into a single [[Row]] with schema [[dataType]]. Firstly, [[initialize()]] is called to initialize
  * intermediate [[MutableAggregationBuffer]]'s with schema [[bufferSchema]]. Then for each input [[Row]],
  * [[update()]] is called to update the corresponding [[MutableAggregationBuffer]] with values from the input [[Row]].
  * Next, [[merge()]] is called to merge all [[MutableAggregationBuffer]]'s belonging to the same group into a
  * single [[MutableAggregationBuffer]]. Finally, [[evaluate()]] is called to convert the merged
  * [[MutableAggregationBuffer]] of each group into a final [[Row]] with schema [[dataType]] as the output of this
  * aggregation function.
  */
class ReportAggregationFunction extends UserDefinedAggregateFunction {

  import ReportAggregationFunction._

  /**
    * A [[StructType]] represents data types of input arguments of this aggregate function.
    * This schema should be identical to the schema in [[co.cask.cdap.report.main.ProgramRunInfoSerializer.SCHEMA]]
    */
  override def inputSchema: StructType = new StructType()
    .add(Constants.NAMESPACE, StringType, false)
    .add(Constants.APPLICATION_NAME, StringType, false)
    .add(Constants.APPLICATION_VERSION, StringType, false)
    .add(Constants.PROGRAM_TYPE, StringType, false)
    .add(Constants.PROGRAM, StringType, false)
    .add(Constants.RUN, StringType, false)
    .add(Constants.STATUS, StringType, false)
    .add(Constants.TIME, LongType, false)
    .add(Constants.MESSAGE_ID, StringType, false)
    // the START_INFO field is nullable, it is only available on records with status STARTING
    .add(Constants.START_INFO, INPUT_START_INFO_SCHEMA, true)

  /**
    * A [[StructType]] represents data types of values in the aggregation buffer.
    * Intermediate aggregation results will be stored in the aggregation buffer. This buffer schema is almost identical
    * to the [[inputSchema]] except that it contains a field [[STATUSES]] with tuples of status and time
    * aggregated from inputs with the same program run ID.
    */
  override def bufferSchema: StructType = new StructType()
    .add(Constants.NAMESPACE, StringType, false)
    .add(Constants.APPLICATION_NAME, StringType, false)
    .add(Constants.APPLICATION_VERSION, StringType, false)
    .add(Constants.PROGRAM_TYPE, StringType, false)
    .add(Constants.PROGRAM, StringType, false)
    .add(Constants.RUN, StringType, false)
    .add(STATUSES, ArrayType(STATUS_TIME_SCHEMA), false)
    // the START_INFO field is nullable, it is only available after the buffer is updated with
    // input row with status STARTING
    .add(Constants.START_INFO, INPUT_START_INFO_SCHEMA, true)

  /**
    * The [[DataType]] of the returned value of this [[UserDefinedAggregateFunction]]. It contains all the possible
    * fields that can be included in a program run report.
    */
  override def dataType: DataType = new StructType()
    .add(Constants.NAMESPACE, StringType, false)
    .add(Constants.ARTIFACT_NAME, StringType, false)
    .add(Constants.ARTIFACT_SCOPE, StringType, false)
    .add(Constants.ARTIFACT_VERSION, StringType, false)
    .add(Constants.APPLICATION_NAME, StringType, false)
    .add(Constants.APPLICATION_VERSION, StringType, false)
    .add(Constants.PROGRAM_TYPE, StringType, false)
    .add(Constants.PROGRAM, StringType, false)
    .add(Constants.RUN, StringType, false)
    .add(Constants.STATUS, StringType, false)
    .add(Constants.START, LongType, false)
    .add(Constants.RUNNING, LongType, true)
    .add(Constants.END, LongType, true)
    .add(Constants.DURATION, LongType, true)
    .add(Constants.USER, StringType, true)
    .add(Constants.START_METHOD, StringType, false)
    .add(Constants.RUNTIME_ARGUMENTS, MapType(StringType, StringType), true)
    .add(Constants.NUM_LOG_WARNINGS, IntegerType, true)
    .add(Constants.NUM_LOG_ERRORS, IntegerType, true)
    .add(Constants.NUM_RECORDS_OUT, IntegerType, true)

  /**
    * Always returns true to indicate this function is deterministic, i.e. given the same input,
    * always return the same output.
    */
  override def deterministic: Boolean = true

  /**
    * Initializes the given aggregation buffer with empty values.
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val bufferRow = new GenericRowWithSchema(buffer.toSeq.toArray, bufferSchema)
    // initialize every String type field with an empty String
    STRING_TYPE_FIELDS.foreach(field => buffer.update(bufferRow.fieldIndex(field), ""))
    // initialize the STATUSES field with an empty Seq
    buffer.update(bufferRow.fieldIndex(STATUSES), Seq.empty[Row])
    // leave the START_INFO field as null
  }

  /**
    * Updates the given aggregation buffer `buffer` with new input data from `input`.
    * For [[STATUSES]] field in the buffer, append status and time from the input to it.
    * Only updates the [[Constants.START_INFO]] field in the `buffer` iff the existing [[Constants.START_INFO]] field
    * is null in the `buffer`.
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val bufferRow = new GenericRowWithSchema(buffer.toSeq.toArray, bufferSchema)
    val inputRow = new GenericRowWithSchema(input.toSeq.toArray, inputSchema)
    updateBufferWithRow(buffer, bufferRow, inputRow, STRING_TYPE_FIELDS)
    // append status and time from the input row to statuses field in the buffer
    buffer.update(bufferRow.fieldIndex(STATUSES),
      bufferRow.getAs[Seq[Row]](STATUSES) :+ Row(inputRow.getAs(Constants.STATUS),
        TimeUnit.MILLISECONDS.toSeconds(inputRow.getAs[Long](Constants.TIME))))
    // Get the StartInfo from the buffer if it exists or construct a new StartInfo from the input row
    if (Option(bufferRow.getAs[Row](Constants.START_INFO)).isEmpty) {
      buffer.update(bufferRow.fieldIndex(Constants.START_INFO), inputRow.getAs[Row](Constants.START_INFO))
    }
  }

  /**
    * For each field in `fields`, `buffer` is updated with the field value in `inputRow` if the `bufferRow`
    * (constructed based on `buffer`) doesn't have a value for this field.
    *
    * @param buffer the buffer to be updated
    * @param bufferRow a row constructed with schema of the `buffer` to be updated
    * @param inputRow the row to get value from
    * @param fields the field names in the buffer as well as column name in the row
    */
  private def updateBufferWithRow(buffer: MutableAggregationBuffer, bufferRow: GenericRowWithSchema,
                                  inputRow: GenericRowWithSchema, fields: Seq[String]): Unit = {
    fields.foreach(field => buffer.update(bufferRow.fieldIndex(field), inputRow.getAs[String](field)))
  }

  /**
    * For each empty field in `buffer1`, update the value of the same field from `buffer2`, except for t
    * he [[STATUSES]] field. [[STATUSES]] field from both buffers are combined.
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer1Row = new GenericRowWithSchema(buffer1.toSeq.toArray, bufferSchema)
    val buffer2Row = new GenericRowWithSchema(buffer2.toSeq.toArray, bufferSchema)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, STRING_TYPE_FIELDS)
    // update statuses in buffer1 by combining the statuses from both buffer1 and buffer2,
    // since STATUSES are initialized as empty Seq, no need to check for empty since ++ with an empty Seq
    // is a legal no-op operation
    buffer1.update(buffer1Row.fieldIndex(STATUSES),
      buffer1Row.getAs[Seq[Row]](STATUSES) ++ buffer2Row.getAs[Seq[Row]](STATUSES))
    // update start info in buffer1 if it's empty
    if (Option(buffer1Row.getAs[Row](Constants.START_INFO)).isEmpty) {
      buffer1.update(buffer1Row.fieldIndex(Constants.START_INFO), buffer2Row.getAs[Row](Constants.START_INFO))
    }
  }

  /**
    * For each field in the given fields, update each field in the first buffer with the String value from
    * the corresponding fields in the second buffer if the field in the first buffer is empty.
    *
    * @param buffer1 the buffer to be updated
    * @param buffer1Row a row constructed with schema from the buffer to be updated
    * @param buffer2Row a row constructed with schema from the second buffer
    * @param fields the field names
    */
  private def mergeBuffers(buffer1: MutableAggregationBuffer, buffer1Row: GenericRowWithSchema,
                           buffer2Row: GenericRowWithSchema, fields: Seq[String]): Unit = {
    fields.foreach(field => if (buffer1Row.getAs[String](field).isEmpty) {
      buffer1.update(buffer1Row.fieldIndex(field), buffer2Row.getAs[String](field))
    })
  }

  /**
    * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
    * aggregation buffer, and return a row with schema [[dataType]], which corresponds to all possible fields in a
    * program run report record.
    */
  override def evaluate(buffer: Row): Row = {
    val bufferRow = new GenericRowWithSchema(buffer.toSeq.toArray, bufferSchema)
    // Construct a status to time map from the list of status time tuples, by keeping the earliest time of a status
    // if there exists multiple times for the same status
    val statusTimeMap = bufferRow.getAs[Seq[Row]](STATUSES).groupBy(_.getAs[String](Constants.STATUS)).map(v =>
      (v._1, v._2.map(_.getAs[Long](Constants.TIME)).min))

    // get the status with maximum time as the status
    val status = statusTimeMap.max(Ordering[Long].on[(_, Long)](_._2))._1
    val start = statusTimeMap.get("STARTING")
    val running = statusTimeMap.get("RUNNING")
    // Get the earliest status with one of the ending statuses
    val end = statusTimeMap.filterKeys(END_STATUSES.contains).values
      .reduceOption(Math.min(_, _)) // avoid compilation error with Math.min(_, _) instead of Math.min
    val startInfo = Option(bufferRow.getAs[Row](Constants.START_INFO))
    val artifactInfo = startInfo.map(_.getAs[Row](Constants.ARTIFACT_ID))
    val duration = end.flatMap(e => start.map(e - _))
    val runtimeArgs = startInfo.map(_.getAs[Map[String, String]](Constants.RUNTIME_ARGUMENTS))
    val startMethod = ProgramStartMethodHelper.getStartMethod(runtimeArgs).name()
    Row(bufferRow.getAs[String](Constants.NAMESPACE),
      artifactInfo.map(_.getAs[String](Constants.ARTIFACT_NAME)).orNull,
      artifactInfo.map(_.getAs[String](Constants.ARTIFACT_SCOPE)).orNull,
      artifactInfo.map(_.getAs[String](Constants.ARTIFACT_VERSION)).orNull,
      bufferRow.getAs[String](Constants.APPLICATION_NAME),
      bufferRow.getAs[String](Constants.APPLICATION_VERSION),
      bufferRow.getAs[String](Constants.PROGRAM_TYPE), bufferRow.getAs[String](Constants.PROGRAM),
      bufferRow.getAs[String](Constants.RUN), status,
      start, running, end, duration, startInfo.map(_.getAs[String](Constants.USER)).orNull,
      // TODO: [CDAP-13397] Use real data for number of records out, number of errors, number of warnings metrics
      startMethod, runtimeArgs.orNull, 0, 0, 0)
  }
}

object ReportAggregationFunction {
  val STATUSES = "statuses"
  val END_STATUSES = Set("COMPLETED", "KILLED", "FAILED")
  val STRING_TYPE_FIELDS = Seq(Constants.NAMESPACE, Constants.APPLICATION_NAME,
    Constants.APPLICATION_VERSION, Constants.PROGRAM_TYPE, Constants.PROGRAM, Constants.RUN)
  val ARTIFACT_SCHEMA: StructType = new StructType()
    .add(Constants.ARTIFACT_NAME, StringType, false)
    .add(Constants.ARTIFACT_SCOPE, StringType, false)
    .add(Constants.ARTIFACT_VERSION, StringType, false)
  val INPUT_START_INFO_SCHEMA: StructType = new StructType()
    // TODO: [CDAP-13541] USER filed is null if authentication is disabled
    .add(Constants.USER, StringType, true)
    .add(Constants.RUNTIME_ARGUMENTS, MapType(StringType, StringType), false)
    .add(Constants.ARTIFACT_ID, ARTIFACT_SCHEMA, false)
  val STATUS_TIME_SCHEMA: StructType = new StructType()
    .add(Constants.STATUS, StringType, false)
    .add(Constants.TIME, LongType)
}
