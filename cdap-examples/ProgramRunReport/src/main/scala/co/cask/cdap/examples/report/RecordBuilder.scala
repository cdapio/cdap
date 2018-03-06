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
package co.cask.cdap.examples.report

import scala.collection.mutable.ListBuffer

class RecordBuilder(private var run: String, private var statuses: scala.collection.mutable.ListBuffer[String]) extends Serializable {

  def this() {
    this("", scala.collection.mutable.ListBuffer())
  }
  def setProgramRunStatus(run: String, status: String): Unit = {
    this.run = run
    this.statuses += status
  }

  def build(): Record = new Record(run, statuses.toList)
  def merge(other: RecordBuilder): RecordBuilder = {
    this.statuses.appendAll(other.getStatuses)
    this
  }

  def getRun: String = run
  def getStatuses: scala.collection.mutable.ListBuffer[String] = statuses
  def setRun(run: String): Unit = this.run = run
  def setStatuses(statuses: scala.collection.mutable.ListBuffer[String]): Unit = this.statuses = statuses
}
