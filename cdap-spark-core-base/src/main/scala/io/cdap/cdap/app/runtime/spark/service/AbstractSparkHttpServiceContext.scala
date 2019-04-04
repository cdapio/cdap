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

package io.cdap.cdap.app.runtime.spark.service

import io.cdap.cdap.api.spark.SparkExecutionContext
import io.cdap.cdap.api.spark.service.SparkHttpServiceContext
import io.cdap.cdap.api.spark.service.SparkHttpServicePluginContext
import io.cdap.cdap.app.runtime.spark.SerializableSparkExecutionContext
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextProvider
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext

/**
  * An abstract base class for implementing [[io.cdap.cdap.api.spark.service.SparkHttpServiceContext]].
  */
abstract class AbstractSparkHttpServiceContext(sec: SparkExecutionContext)
  extends SerializableSparkExecutionContext(sec) with SparkHttpServiceContext {

  override lazy val getSparkContext: SparkContext = SparkRuntimeEnv.waitForContext

  override lazy val getJavaSparkContext: JavaSparkContext = new JavaSparkContext(getSparkContext)

  override def getPluginContext: SparkHttpServicePluginContext =
    new DefaultSparkHttpServicePluginContext(SparkRuntimeContextProvider.get())
}
