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

import co.cask.cdap.api.schedule.ProgramStatusTriggerInfo
import co.cask.cdap.api.spark.AbstractSpark
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkMain
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Unit test for accessing triggering schedule info from Spark program launched by schedule.
  */
final class TriggeredSpark extends AbstractSpark with SparkMain {
  override def configure(): Unit = {
    setDescription("Test Spark with Triggered Workflow")
    setMainClass(classOf[TriggeredSpark])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val triggerInfo = sec.getTriggeringScheduleInfo.get.getTriggerInfos.get(0).asInstanceOf[ProgramStatusTriggerInfo]

    val sparkConf = new SparkConf

    // Optionally set the serializer
    Option(triggerInfo.getRuntimeArguments.get("spark.serializer")).foreach(
      sparkConf.set("spark.serializer", _)
    )

    val sc = new SparkContext(sparkConf)
    val sum = sc.parallelize(Seq(1, 2, 3, 4, 5))
      .filter(_ => triggerInfo.getProgram().equals("ScalaClassicSpark")).reduce(_ + _)
    require(sum == 15)
  }
}
