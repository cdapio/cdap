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

import co.cask.cdap.api.schedule.{TriggerInfo, TriggeringScheduleInfo}
import co.cask.cdap.report.util.{Constants, TriggeringScheduleInfoAdapter}
import com.google.gson.GsonBuilder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
  * An aggregation function that aggregates [[Row]]'s with the same program run ID into intermediate
  * [[MutableAggregationBuffer]]'s and finally merge the [[MutableAggregationBuffer]]'s to build a single [[Row]]
  */
class ReportAggregationFunction extends UserDefinedAggregateFunction {
  import ReportAggregationFunction._

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
    .add(Constants.START_INFO, new StructType()
      .add(Constants.USER, StringType, true)
      .add(Constants.RUNTIME_ARGUMENTS, MapType(StringType, StringType), false)
      .add(Constants.ARTIFACT_ID, new StructType()
        .add(Constants.ARTIFACT_NAME, StringType, false)
        .add(Constants.ARTIFACT_SCOPE, StringType, false)
        .add(Constants.ARTIFACT_VERSION, StringType, false), false),
      true)

  override def bufferSchema: StructType = new StructType()
    .add(Constants.NAMESPACE, StringType, false)
    .add(Constants.APPLICATION_NAME, StringType, false)
    .add(Constants.APPLICATION_VERSION, StringType, false)
    .add(Constants.PROGRAM_TYPE, StringType, false)
    .add(Constants.PROGRAM, StringType, false)
    .add(Constants.RUN, StringType, false)
    .add(STATUSES, ArrayType(new StructType()
      .add(Constants.STATUS, StringType, false)
      .add(Constants.TIME, LongType)), false)
    .add(Constants.START_INFO, new StructType()
      .add(Constants.USER, StringType, true)
      .add(Constants.RUNTIME_ARGUMENTS, MapType(StringType, StringType), false)
      .add(Constants.ARTIFACT_NAME, StringType, false)
      .add(Constants.ARTIFACT_SCOPE, StringType, false)
      .add(Constants.ARTIFACT_VERSION, StringType, false),
      true)

  override def dataType: DataType = new StructType()
    .add(Constants.NAMESPACE, StringType, false)
    .add(Constants.ARTIFACT_NAME, StringType, true)
    .add(Constants.ARTIFACT_SCOPE, StringType, true)
    .add(Constants.ARTIFACT_VERSION, StringType, true)
    .add(Constants.APPLICATION_NAME, StringType, false)
    .add(Constants.APPLICATION_VERSION, StringType, false)
    .add(Constants.PROGRAM_TYPE, StringType, false)
    .add(Constants.PROGRAM, StringType, false)
    .add(Constants.RUN, StringType, false)
    .add(Constants.STATUS, StringType, false)
    .add(Constants.START, LongType, true)
    .add(Constants.RUNNING, LongType, true)
    .add(Constants.END, LongType, true)
    .add(Constants.DURATION, LongType, true)
    .add(Constants.USER, StringType, true)
    .add(Constants.START_METHOD, StringType, false)
    .add(Constants.RUNTIME_ARGUMENTS, MapType(StringType, StringType), true)
    .add(Constants.NUM_LOG_WARNINGS, IntegerType, true)
    .add(Constants.NUM_LOG_ERRORS, IntegerType, true)
    .add(Constants.NUM_RECORDS_OUT, IntegerType, true)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val bufferRow = new GenericRowWithSchema(buffer.toSeq.toArray, bufferSchema)
    buffer.update(bufferRow.fieldIndex(Constants.NAMESPACE), "")
    buffer.update(bufferRow.fieldIndex(Constants.APPLICATION_NAME), "")
    buffer.update(bufferRow.fieldIndex(Constants.APPLICATION_VERSION), "")
    buffer.update(bufferRow.fieldIndex(Constants.PROGRAM_TYPE), "")
    buffer.update(bufferRow.fieldIndex(Constants.PROGRAM), "")
    buffer.update(bufferRow.fieldIndex(Constants.RUN), "")
    buffer.update(bufferRow.fieldIndex(STATUSES), Seq.empty[Row])
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val bufferRow = new GenericRowWithSchema(buffer.toSeq.toArray, bufferSchema)
    val row = new GenericRowWithSchema(input.toSeq.toArray, inputSchema)
    updateBufferWithRow(buffer, bufferRow, row, Constants.NAMESPACE)
    updateBufferWithRow(buffer, bufferRow, row, Constants.APPLICATION_NAME)
    updateBufferWithRow(buffer, bufferRow, row, Constants.APPLICATION_VERSION)
    updateBufferWithRow(buffer, bufferRow, row, Constants.PROGRAM_TYPE)
    updateBufferWithRow(buffer, bufferRow, row, Constants.PROGRAM)
    updateBufferWithRow(buffer, bufferRow, row, Constants.RUN)
    // append status and time from the input row to statuses field in the buffer
    buffer.update(bufferRow.fieldIndex(STATUSES),
      bufferRow.getAs[Seq[Row]](STATUSES) :+ Row(row.getAs(Constants.STATUS),
      TimeUnit.MILLISECONDS.toSeconds(row.getAs[Long](Constants.TIME))))
    // Get the StartInfo from the buffer if it exists or construct a new StartInfo from the input row
    val startInfo = Option(bufferRow.getAs[Row](Constants.START_INFO))
      .orElse(Option(row.getAs[Row](Constants.START_INFO)).map(rowToStartInfo))
    buffer.update(bufferRow.fieldIndex(Constants.START_INFO), startInfo)
  }

  /**
    * Updates a field in the buffer with the String value from the corresponding column in the given row.
    *
    * @param buffer the buffer to be updated
    * @param bufferRow a row constructed with schema from the buffer to be updated
    * @param row the row to get value from
    * @param field the field name in the buffer as well as column name in the row
    */
  private def updateBufferWithRow(buffer: MutableAggregationBuffer, bufferRow: GenericRowWithSchema,
                                  row: GenericRowWithSchema, field: String): Unit = {
    buffer.update(bufferRow.fieldIndex(field), row.getAs[String](field))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer1Row = new GenericRowWithSchema(buffer1.toSeq.toArray, bufferSchema)
    val buffer2Row = new GenericRowWithSchema(buffer2.toSeq.toArray, bufferSchema)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, Constants.NAMESPACE)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, Constants.APPLICATION_NAME)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, Constants.APPLICATION_VERSION)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, Constants.PROGRAM_TYPE)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, Constants.PROGRAM)
    mergeBuffers(buffer1, buffer1Row, buffer2Row, Constants.RUN)
    // update statuses in buffer1 by combining the statuses from both buffer1 and buffer2
    buffer1.update(buffer1Row.fieldIndex(STATUSES),
      buffer1Row.getAs[Seq[Row]](STATUSES) ++ buffer2Row.getAs[Seq[Row]](STATUSES))
    // update start info in buffer1 if it's empty
    if (Option(buffer1Row.getAs[Row](Constants.START_INFO)).isEmpty) {
      buffer1.update(buffer1Row.fieldIndex(Constants.START_INFO), buffer2Row.getAs[Row](Constants.START_INFO))
    }
  }


  /**
    * Update the field in the first buffer with the String value from the corresponding field in the second
    * buffer if the field in the first buffer is empty.
    *
    * @param buffer1 the buffer to be updated
    * @param buffer1Row a row constructed with schema from the buffer to be updated
    * @param buffer2Row a row constructed with schema from the second buffer
    * @param field the field name
    */
  private def mergeBuffers(buffer1: MutableAggregationBuffer, buffer1Row: GenericRowWithSchema,
                           buffer2Row: GenericRowWithSchema, field: String): Unit = {
    if (buffer1Row.getAs[String](field).isEmpty) {
      buffer1.update(buffer1Row.fieldIndex(field), buffer2Row.getAs[String](field))
    }
  }

  override def evaluate(buffer: Row): Row = {
    val bufferRow = new GenericRowWithSchema(buffer.toSeq.toArray, bufferSchema)
    // Construct a status to time map from the list of status time tuples, by keeping the earliest time of a status
    // if there exists multiple times for the same status
    val statusTimeMap = bufferRow.getAs[Seq[Row]](STATUSES).groupBy(_.getAs[String](Constants.STATUS)).map(v =>
      (v._1, v._2.map(_.getAs[Long](Constants.TIME)).min))

    // get the status with maximum time as the status
    val status = statusTimeMap.max(Ordering[Long].on[(_,Long)](_._2))._1
    val start = statusTimeMap.get("STARTING")
    val running = statusTimeMap.get("RUNNING")
    // Get the earliest status with one of the ending statuses
    val end = statusTimeMap.filterKeys(END_STATUSES.contains).values
      .reduceOption(Math.min(_, _)) // avoid compilation error with Math.min(_, _) instead of Math.min
    val startInfo = Option(bufferRow.getAs[Row](Constants.START_INFO))
    val duration = end.flatMap(e => start.map(e - _))
    val runtimeArgs = startInfo.map(_.getAs[Map[String, String]](Constants.RUNTIME_ARGUMENTS))
    val startMethod = getStartMethod(runtimeArgs)
    val r = Row(bufferRow.getAs[String](Constants.NAMESPACE),
      startInfo.map(_.getAs[String](Constants.ARTIFACT_NAME)).orNull,
      startInfo.map(_.getAs[String](Constants.ARTIFACT_VERSION)).orNull,
      startInfo.map(_.getAs[String](Constants.ARTIFACT_SCOPE)).orNull,
      bufferRow.getAs[String](Constants.APPLICATION_NAME),
      bufferRow.getAs[String](Constants.APPLICATION_VERSION),
      bufferRow.getAs[String](Constants.PROGRAM_TYPE), bufferRow.getAs[String](Constants.PROGRAM),
      bufferRow.getAs[String](Constants.RUN), status,
      start, running, end, duration, startInfo.map(_.getAs[String](Constants.USER)).orNull,
      startMethod, runtimeArgs.orNull, 0, 0, 0)
    LOG.trace("RecordBuilder = {}", buffer)
    LOG.trace("Record = {}", r)
    r
  }

  private def getStartMethod(runtimeArgs: Option[scala.collection.Map[String, String]]): String = {
    if (runtimeArgs.isEmpty) return MANUAL
    val scheduleInfoJson = runtimeArgs.get.get(SCHEDULE_INFO_KEY)
    if (scheduleInfoJson.isEmpty) return MANUAL
    val scheduleInfo: TriggeringScheduleInfo = GSON.fromJson(scheduleInfoJson.get, classOf[TriggeringScheduleInfo])
    val triggers = scheduleInfo.getTriggerInfos
    if (Option(triggers).isEmpty || triggers.isEmpty) return MANUAL
    triggers.get(0).getType match {
      case TriggerInfo.Type.TIME => SCHEDULED
      case _ => TRIGGERED
    }
  }

  private def rowToStartInfo(startInfoRow: Row): Row = {
    val artifact: Row = startInfoRow.getAs[Row](Constants.ARTIFACT_ID)
    Row(startInfoRow.getAs[String](Constants.USER),
      startInfoRow.getAs[scala.collection.Map[String, String]](Constants.RUNTIME_ARGUMENTS),
      artifact.getAs[String](Constants.ARTIFACT_NAME), artifact.getAs[String](Constants.ARTIFACT_VERSION),
      artifact.getAs[String](Constants.ARTIFACT_SCOPE))
  }
}

object ReportAggregationFunction {
  val LOG = LoggerFactory.getLogger(ReportAggregationFunction.getClass)
  val STATUSES = "statuses"
  val END_STATUSES = Set("COMPLETED", "KILLED", "FAILED")
  val MANUAL = "MANUAL"
  val SCHEDULED = "SCHEDULED"
  val TRIGGERED = "TRIGGERED"
  val SCHEDULE_INFO_KEY = "triggeringScheduleInfo"
  val GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder).create()
}
