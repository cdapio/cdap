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

case class RecordBuilder(program: String, run: String, statuses: scala.collection.Seq[(String, Long)], startInfo: Option[StartInfo]) {

//  def this() {
//    this("", "", Vector.empty)
//  }

  def merge(other: RecordBuilder): RecordBuilder = {
    val program = if (this.program.isEmpty) other.program else this.program
    val run = if (this.run.isEmpty) other.run else this.run
    val statuses = this.statuses ++ other.statuses
    val startInfo = if (this.startInfo.isEmpty) other.startInfo else this.startInfo
//    println("Other = %s".format(other))
//    println("This = %s".format(this))
    val r = RecordBuilder(program, run, statuses, startInfo)
//    println("===> Merged = %s".format(r))
    r
  }

  def build(): Record = {
    val statusTimeMap = statuses.groupBy(_._1).map(v => (v._1, v._2.map(_._2).min))
    val user = if (startInfo.isDefined) Some(startInfo.get.user) else None
    val runtimeArgs = if (startInfo.isDefined) Some(startInfo.get.runtimeArgs) else None
//    println("this = %s".format(this))
    val r = Record(program, run, statusTimeMap.get("STARTING"), statusTimeMap.get("RUNNING"),
       statusTimeMap.get("COMPLETED"), user, runtimeArgs)
//    println("===> build %s".format(r))
    r
  }
}

case class StartInfo(user: String, runtimeArgs: scala.collection.Map[String, String])