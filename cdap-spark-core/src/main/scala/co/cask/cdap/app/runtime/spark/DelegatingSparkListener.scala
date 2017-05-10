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

import java.util

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

import scala.collection.JavaConversions._

/**
  * Spark1 DelegatingSparkListener, which adds a 'onOtherEvent' method for a specific cdh version of spark.
  */
class DelegatingSparkListener(override val sparkListeners: util.Collection[SparkListener])
  extends DelegatingSparkListenerTrait {
  
  def onOtherEvent(event: SparkListenerEvent) = {
    // no-op
  }
}
