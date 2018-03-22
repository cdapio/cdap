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

import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.mapAsScalaMap

/**
  * Builder for creating a [[Record]].
  *
  * @param namespace namespace of the program run
  * @param program program name
  * @param run run ID
  * @param statusTimes status and time tuples indicating the time when each status of the program is reached
  * @param startInfo the information obtained when a program run starts
  */
case class RecordBuilder(namespace: String, program: String, run: String,
                         statusTimes: scala.collection.Seq[(String, Long)], startInfo: Option[StartInfo]) {
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
    val program = if (this.program.isEmpty) other.program else this.program
    val run = if (this.run.isEmpty) other.run else this.run
    val statusTimes = this.statusTimes ++ other.statusTimes
    val startInfo = if (this.startInfo.isEmpty) other.startInfo else this.startInfo
    val r = RecordBuilder(namespace, program, run, statusTimes, startInfo)
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
    val start = statusTimeMap.get("STARTING")
    val running = statusTimeMap.get("RUNNING")
    // Get the earliest status with one of the ending status COMPLETED, KILLED and FAILED
    val endTime = statusTimeMap.getOrElse("COMPLETED", Long.MaxValue)
      .min(statusTimeMap.getOrElse("KILLED", Long.MaxValue))
      .min(statusTimeMap.getOrElse("FAILED", Long.MaxValue))
    val end = if (endTime == Long.MaxValue) None else Some(endTime)
    val duration = if (start.isDefined && end.isDefined) Some(end.get - start.get) else None
    val user = if (startInfo.isDefined) Some(startInfo.get.user) else None
    val runtimeArgs = if (startInfo.isDefined) Some(startInfo.get.runtimeArgs) else None
    val r = Record(namespace, program, run, start, running, end, duration, user, runtimeArgs)
    LOG.trace("RecordBuilder={}", this)
    LOG.trace("Record = {}", r)
    r
  }
}

/**
  * Represents the information obtained when a program run starts.
  *
  * @param user the user who starts the program run
  * @param runtimeArgs runtime arguments of the program run
  */
case class StartInfo(user: String, runtimeArgs: scala.collection.Map[String, String]) {
  def this(user: String, runtimeArgs: java.util.Map[String, String]) = this(user, mapAsScalaMap(runtimeArgs).toMap)
}

object RecordBuilder {
  val LOG = LoggerFactory.getLogger(RecordBuilder.getClass)
}
