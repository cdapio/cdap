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

package co.cask.cdap.app.runtime.spark.service

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.slf4j.LoggerFactory

/**
  * A [[org.apache.spark.scheduler.SparkListener]] to optionally start a
  * [[co.cask.cdap.app.runtime.spark.service.SparkHttpServiceServer]] if
  * [[co.cask.cdap.api.spark.service.SparkHttpServiceHandler]] is used.
  */
class SparkHttpServiceStartListener extends SparkListener {

  import SparkHttpServiceStartListener._

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    LOG.info("Application started")
  }
}

object SparkHttpServiceStartListener {

  private val LOG = LoggerFactory.getLogger(classOf[SparkHttpServiceStartListener])
}
