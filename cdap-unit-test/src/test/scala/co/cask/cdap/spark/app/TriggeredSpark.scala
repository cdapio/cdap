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

package co.cask.cdap.spark.app

import co.cask.cdap.api.schedule.{ProgramStatusTriggerInfo, TriggeringScheduleInfo}
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import com.google.common.base.Preconditions

/**
  * Unit test for accessing triggering schedule info from Spark program launched by schedule.
  */
final class TriggeredSpark extends AbstractSpark with SparkMain {
  override def configure(): Unit = {
    setDescription("Test Spark with Triggered Workflow")
    setMainClass(classOf[TriggeredSpark])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val scheduleInfoOption: Option[TriggeringScheduleInfo] = sec.getTriggeringScheduleInfo
    Preconditions.checkState(scheduleInfoOption.isDefined)
    val scheduleInfo: TriggeringScheduleInfo = scheduleInfoOption.get
    val triggerInfo: ProgramStatusTriggerInfo =
      scheduleInfo.getTriggerInfos.get(0).asInstanceOf[ProgramStatusTriggerInfo]
    Preconditions.checkState(classOf[TestSparkApp].getSimpleName
      .equals(triggerInfo.getApplicationSpecification.getName))
  }
}
