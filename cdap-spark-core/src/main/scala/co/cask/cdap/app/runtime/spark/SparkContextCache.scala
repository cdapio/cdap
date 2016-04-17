/*
 * Copyright © 2016 Cask Data, Inc.
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

import java.util.concurrent.ConcurrentLinkedQueue
import javax.annotation.Nullable

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

/**
  * A singleton cache for [[org.apache.spark.SparkContext]] and [[org.apache.spark.streaming.StreamingContext]]
  * to provide universal access to those contexts created by the user Spark program within CDAP.
  *
  * With ClassLoader isolation, there will be one instance of this class per one Spark program execution.
  */
object SparkContextCache {

  private var stopped = false
  private var sparkContext: Option[SparkContext] = None
  private var streamingContext: Option[StreamingContext] = None
  private val sparkListeners = new ConcurrentLinkedQueue[SparkListener]()

  /**
    * Adds a [[org.apache.spark.scheduler.SparkListener]]. The given listener will be added to
    * [[org.apache.spark.SparkContext]] when it becomes available.
    */
  def addSparkListener(listener: SparkListener): Unit = sparkListeners.add(listener)

  /**
    * Sets the [[org.apache.spark.SparkContext]] for the execution.
    */
  def setContext(context: SparkContext): Unit = {
    this.synchronized {
      if (stopped) {
        context.stop()
        throw new IllegalStateException("Spark program is already stopped")
      }

      if (sparkContext.isDefined) {
        throw new IllegalStateException("SparkContext was already created")
      }

      sparkContext = Some(context)
      context.addSparkListener(new DelegatingSparkListener)
    }
  }

  /**
    * Sets the [[org.apache.spark.streaming.StreamingContext]] that is currently in use.
    */
  def setContext(context: StreamingContext): Unit = {
    this.synchronized {
      if (stopped) {
        context.stop(false)
        throw new IllegalStateException("Spark program is already stopped")
      }

      // Spark doesn't allow multiple StreamingContext instances concurrently, hence we don't need to check in here
      streamingContext = Some(context)
    }
  }

  /**
    * Returns the current [[org.apache.spark.SparkContext]].
    *
    * @throws IllegalStateException if there is no SparkContext available.
    */
  def getContext: SparkContext = {
    this.synchronized {
      sparkContext.getOrElse(throw new IllegalStateException("SparkContext is not available"))
    }
  }

  /**
    * Sets a local property on the [[org.apache.spark.SparkContext]] object if available.
    *
    * @param key key of the property
    * @param value value of the property
    * @return `true` if successfully set the property; `false` otherwise
    */
  def setLocalProperty(key: String, value: String): Boolean = {
    this.synchronized {
      sparkContext.fold(false)(context => {
        context.setLocalProperty(key, value)
        true
      })
    }
  }

  /**
    * Gets a local property from the [[org.apache.spark.SparkContext]] object if available.
    *
    * @param key key of the property
    * @return the value of property of `null` if either there is no SparkContext or the key doesn't exist
    */
  @Nullable
  def getLocalProperty(key: String): String = {
    this.synchronized {
      sparkContext.map(_.getLocalProperty(key)).orNull
    }
  }

  /**
    * Stop this cache. It will stop the [[org.apache.spark.SparkContext]] in the cache and also prevent any
    * future setting of new [[org.apache.spark.SparkContext]].
    *
    * @return [[scala.Some]] [[org.apache.spark.SparkContext]] if there is a one
    */
  def stop: Option[SparkContext] = {
    this.synchronized {
      if (!stopped) {
        stopped = true
        try {
          streamingContext.foreach(ssc => {
            ssc.stop(false)
            ssc.awaitTermination()
          })
        } finally {
          sparkContext.foreach(_.stop())
        }
      }
      sparkContext;
    }
  }

  /**
    * A delegating [[org.apache.spark.scheduler.SparkListener]] that simply dispatch all callbacks to a list
    * of [[org.apache.spark.scheduler.SparkListener]].
    */
  private class DelegatingSparkListener extends SparkListener {

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
}