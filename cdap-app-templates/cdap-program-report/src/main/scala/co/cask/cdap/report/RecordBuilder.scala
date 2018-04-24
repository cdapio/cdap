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

import co.cask.cdap.api.schedule.{TriggerInfo, TriggeringScheduleInfo}
import co.cask.cdap.report.util.TriggeringScheduleInfoAdapter
import com.google.gson.{Gson, GsonBuilder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Builder for creating a [[Record]].
  *
  * @param namespace namespace of the program run
  * @param program program name
  * @param run run ID
  * @param statusTimes status and time tuples indicating the time when each status of the program is reached
  * @param startInfo the information obtained when a program run starts
  */
case class RecordBuilder(namespace: String, applicationName: String, applicationVersion: String,
                         programType: String, program: String, run: String,
                         statusTimes: Seq[(String, Long)], startInfo: Option[StartInfo],
                         numLogWarnings: Int, numLogErrors: Int, numRecordsOut: Int) {
  import RecordBuilder._
  /**
    * Merges the contents of this with the other [[RecordBuilder]] by replacing empty values in this with
    * values from the other.
    *
    * @param other the [[RecordBuilder]] to combine this with
    * @return a new [[RecordBuilder]] combining this and other
    */
  def merge(other: RecordBuilder): RecordBuilder = {
    val namespace = if (this.namespace.isEmpty) other.namespace else this.namespace
    val applicationName = if (this.applicationName.isEmpty) other.applicationName else this.applicationName
    val applicationVersion = if (this.applicationVersion.isEmpty) other.applicationVersion else this.applicationVersion
    val programType = if (this.programType.isEmpty) other.programType else this.programType
    val program = if (this.program.isEmpty) other.program else this.program
    val run = if (this.run.isEmpty) other.run else this.run
    val statusTimes = this.statusTimes ++ other.statusTimes
    val startInfo = if (this.startInfo.isEmpty) other.startInfo else this.startInfo
    val r = RecordBuilder(namespace, applicationName, applicationVersion, programType, program, run,
      statusTimes, startInfo, 0, 0, 0)
    LOG.trace("Merged this {} with other {} to get a new {}", this, other, r)
    r
  }

  /**
    * @return a [[Record]] built from the information in this [[RecordBuilder]]
    */
  def build(): Record = {
    import ReportGenerationHelper._
    // Construct a status to time map from the list of status time tuples, by keeping the earliest time of a status
    // if there exists multiple times for the same status
    val statusTimeMap = statusTimes.groupBy(_._1).map(v => (v._1, v._2.map(_._2).min))
    // get the status with maximum time as the status
    val status = statusTimeMap.max(Ordering[Long].on[(_,Long)](_._2))._1
    val start = statusTimeMap.get("STARTING")
    val running = statusTimeMap.get("RUNNING")
    // Get the earliest status with one of the ending statuses
    val end = statusTimeMap.filterKeys(END_STATUSES.contains).values
      .reduceOption(Math.min(_, _)) // avoid compilation error with Math.min(_, _) instead of Math.min
    val duration = end.flatMap(e => start.map(e - _))
    val runtimeArgs = startInfo.map(_.runtimeArgs)
    val startMethod = getStartMethod(runtimeArgs)
    val r = Record(namespace,
      startInfo.map(_.artifactName), startInfo.map(_.artifactVersion), startInfo.map(_.artifactScope),
      applicationName, applicationVersion,
      programType, program, run, status, start, running, end, duration, startInfo.map(_.user), startMethod, runtimeArgs,
      numLogWarnings, numLogErrors, numRecordsOut)
    LOG.trace("RecordBuilder = {}", this)
    LOG.trace("Record = {}", r)
    r
  }

  private def getStartMethod(runtimeArgs: Option[scala.collection.Map[String, String]]): String = {
    if (runtimeArgs.isEmpty) return MANUAL
    val scheduleInfoJson = runtimeArgs.get.get("triggeringScheduleInfo")
    if (scheduleInfoJson.isEmpty) return MANUAL
    val scheduleInfo: TriggeringScheduleInfo = GSON.fromJson(scheduleInfoJson.get, classOf[TriggeringScheduleInfo])
    val triggers = scheduleInfo.getTriggerInfos
    if (triggers.isEmpty) return MANUAL
    triggers.get(0).getType match {
      case TriggerInfo.Type.TIME => "SCHEDULED"
      case _ => "TRIGGERED"
    }
  }
}

/**
  * Represents the information obtained when a program run starts.
  *
  * @param user the user who starts the program run
  * @param runtimeArgs runtime arguments of the program run
  */

case class StartInfo(user: String,
                     // Use scala.collection.Map to avoid compilation error in Janino generated code
                     runtimeArgs: scala.collection.Map[String, String],
                     artifactName: String,  artifactVersion: String, artifactScope: String) {
  def this(user: String, runtimeArgs: java.util.Map[String, String],
           artifactName: String, artifactVersion: String, artifactScope: String) =
    this(user, mapAsScalaMap(runtimeArgs).toMap, artifactName, artifactVersion, artifactScope)
  def getRuntimeArgsAsJavaMap(): java.util.Map[String, String] = runtimeArgs
}

object RecordBuilder {
  val LOG = LoggerFactory.getLogger(RecordBuilder.getClass)
  val END_STATUSES = Set("COMPLETED", "KILLED", "FAILED")
  val MANUAL = "MANUAL"
  val GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder).create()
}
