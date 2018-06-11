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
package co.cask.cdap.report.util

import co.cask.cdap.api.schedule.{TriggerInfo, TriggeringScheduleInfo}
import co.cask.cdap.report.proto.ProgramRunStartMethod
import com.google.gson.GsonBuilder

/**
  * A helper class for determining the start method of a program run.
  */
object ProgramStartMethodHelper {
  val GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder).create()

  /**
    * Returns how the program run was started
    *
    * @param runtimeArgs the runtime arguments of the program run
    * @return one of the methods [[ProgramRunStartMethod.MANUAL]], [[ProgramRunStartMethod.SCHEDULED]]
    *         and [[ProgramRunStartMethod.TRIGGERED]] each indicating that the program run
    *         was started manually, scheduled by time, or triggered by certain condition such as new dataset partition
    *         and program status.
    */
  def getStartMethod(runtimeArgs: Option[scala.collection.Map[String, String]]): ProgramRunStartMethod = {
    if (runtimeArgs.isEmpty) return ProgramRunStartMethod.MANUAL
    val scheduleInfoJson = runtimeArgs.get.get(Constants.Notification.SCHEDULE_INFO_KEY)
    if (scheduleInfoJson.isEmpty) return ProgramRunStartMethod.MANUAL
    val scheduleInfo: TriggeringScheduleInfo = GSON.fromJson(scheduleInfoJson.get, classOf[TriggeringScheduleInfo])
    val triggers = scheduleInfo.getTriggerInfos
    if (Option(triggers).isEmpty || triggers.isEmpty) return ProgramRunStartMethod.MANUAL
    triggers.get(0).getType match {
      case TriggerInfo.Type.TIME => ProgramRunStartMethod.SCHEDULED
      case _ => ProgramRunStartMethod.TRIGGERED
    }
  }
}
