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

package co.cask.cdap.datapipeline

import co.cask.cdap.api.spark.{SparkExecutionContext, SparkMain}
import co.cask.cdap.etl.batch.BatchPhaseSpec
import co.cask.cdap.etl.common.{BasicArguments, Constants, DefaultMacroEvaluator}
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext
import com.google.gson.Gson

/**
 * Instantiates a SparkMain plugin and runs it.
 */
class ScalaSparkMainWrapper extends SparkMain {
  val GSON = new Gson()

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val stageName = sec.getSpecification.getProperty(ExternalSparkProgram.STAGE_NAME)
    val batchPhaseSpec = GSON.fromJson(sec.getSpecification.getProperty(Constants.PIPELINEID), classOf[BatchPhaseSpec])
    val pluginContext = new SparkPipelinePluginContext(sec.getPluginContext, sec.getMetrics,
                                                       batchPhaseSpec.isStageLoggingEnabled,
                                                       batchPhaseSpec.isProcessTimingEnabled)

    val macroEvaluator =
      new DefaultMacroEvaluator(new BasicArguments(sec.getWorkflowToken.orNull, sec.getRuntimeArguments),
        sec.getLogicalStartTime, sec.getSecureStore, sec.getNamespace)
    val sparkMain: SparkMain = pluginContext.newPluginInstance(stageName, macroEvaluator)
    sparkMain.run(sec)
  }
}
