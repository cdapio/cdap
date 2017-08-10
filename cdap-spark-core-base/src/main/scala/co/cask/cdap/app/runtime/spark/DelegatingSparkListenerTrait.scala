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

package co.cask.cdap.app.runtime.spark

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate
import org.apache.spark.scheduler.SparkListenerExecutorRemoved
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerTaskGettingResult
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerUnpersistRDD

import java.util

import scala.collection.JavaConversions._

/**
  * Common logic across Spark1 and Spark2 for a SparkListener. Delegates all methods to a list of delegates.
  * The list may be mutated outside of this class.
  */
trait DelegatingSparkListenerTrait extends SparkListener {

  def sparkListeners: util.Collection[SparkListener]

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) =
    sparkListeners.foreach(_.onStageCompleted(stageCompleted))

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
    sparkListeners.foreach(_.onStageSubmitted(stageSubmitted))

  override def onTaskStart(taskStart: SparkListenerTaskStart) =
    sparkListeners.foreach(_.onTaskStart(taskStart))

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) =
    sparkListeners.foreach(_.onTaskGettingResult(taskGettingResult))

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) =
    sparkListeners.foreach(_.onTaskEnd(taskEnd))

  override def onJobStart(jobStart: SparkListenerJobStart) =
    sparkListeners.foreach(_.onJobStart(jobStart))

  override def onJobEnd(jobEnd: SparkListenerJobEnd) =
    sparkListeners.foreach(_.onJobEnd(jobEnd))

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) =
    sparkListeners.foreach(_.onEnvironmentUpdate(environmentUpdate))

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) =
    sparkListeners.foreach(_.onBlockManagerAdded(blockManagerAdded))

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) =
    sparkListeners.foreach(_.onBlockManagerRemoved(blockManagerRemoved))

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) =
    sparkListeners.foreach(_.onUnpersistRDD(unpersistRDD))

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) =
    sparkListeners.foreach(_.onApplicationStart(applicationStart))

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) =
    sparkListeners.foreach(_.onApplicationEnd(applicationEnd))

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) =
    sparkListeners.foreach(_.onExecutorMetricsUpdate(executorMetricsUpdate))

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) =
    sparkListeners.foreach(_.onExecutorAdded(executorAdded))

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) =
    sparkListeners.foreach(_.onExecutorRemoved(executorRemoved))
}
