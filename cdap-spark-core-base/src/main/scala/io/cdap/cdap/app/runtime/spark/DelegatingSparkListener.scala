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

package io.cdap.cdap.app.runtime.spark

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerEvent

import java.util

import scala.collection.JavaConversions._

/**
  * Spark2 DelegatingSparkListener, which override new 'onOtherEvent' method.
  */
class DelegatingSparkListener extends DelegatingSparkListenerTrait {

  override def onOtherEvent(event: SparkListenerEvent) = sparkListeners.foreach(_.onOtherEvent(event))

  override def sparkListeners: Seq[SparkListener] = SparkRuntimeEnv.getSparkListeners()
}
