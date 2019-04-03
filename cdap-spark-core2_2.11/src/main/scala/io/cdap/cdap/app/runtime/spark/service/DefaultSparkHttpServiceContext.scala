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

import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.app.runtime.spark.SparkRuntimeEnv
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

/**
  * Default implementation of the [[co.cask.cdap.api.spark.service.SparkHttpServiceContext]].
  */
class DefaultSparkHttpServiceContext(sec: SparkExecutionContext) extends AbstractSparkHttpServiceContext(sec) {

  override lazy val getSparkSession: SparkSession = new SQLContext(SparkRuntimeEnv.waitForContext).sparkSession
}
