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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spark.plugin.SparkPipelinePluginContext;
import com.google.gson.Gson;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * This class is a wrapper for the vanilla spark programs.
 */
public class JavaSparkMainWrapper implements JavaSparkMain {
  private static final Gson GSON = new Gson();

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    String stageName = sec.getSpecification().getProperty(ExternalSparkProgram.STAGE_NAME);
    BatchPhaseSpec batchPhaseSpec = GSON.fromJson(sec.getSpecification().getProperty(Constants.PIPELINEID),
                                                  BatchPhaseSpec.class);
    PipelinePluginContext pluginContext = new SparkPipelinePluginContext(sec.getPluginContext(), sec.getMetrics(),
                                                                         batchPhaseSpec.isStageLoggingEnabled(),
                                                                         batchPhaseSpec.isProcessTimingEnabled());

    Class<?> mainClass = pluginContext.loadPluginClass(stageName);

    // if it's a CDAP JavaSparkMain, instantiate it and call the run method
    if (JavaSparkMain.class.isAssignableFrom(mainClass)) {
      MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(sec.getWorkflowToken(), sec.getRuntimeArguments(),
                                                                sec.getLogicalStartTime(), sec.getSecureStore(),
                                                                sec.getNamespace());
      JavaSparkMain javaSparkMain = pluginContext.newPluginInstance(stageName, macroEvaluator);
      javaSparkMain.run(sec);
    } else {
      // otherwise, assume there is a 'main' method and call it
      String programArgs = getProgramArgs(sec, stageName);
      String[] args = programArgs == null ?
        RuntimeArguments.toPosixArray(sec.getRuntimeArguments()) : programArgs.split(" ");
      final Method mainMethod = mainClass.getMethod("main", String[].class);
      final Object[] methodArgs = new Object[1];
      methodArgs[0] = args;
      Caller caller = pluginContext.getCaller(stageName);
      caller.call(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          mainMethod.invoke(null, methodArgs);
          return null;
        }
      });
    }
  }

  @Nullable
  private String getProgramArgs(JavaSparkExecutionContext sec, String stageName) {
    // get program args from plugin properties
    PluginProperties pluginProperties = sec.getPluginContext().getPluginProperties(stageName);
    String programArgs = pluginProperties == null ?
      null : pluginProperties.getProperties().get(ExternalSparkProgram.PROGRAM_ARGS);

    // can be overridden by runtime args
    String programArgsKey = stageName + "." + ExternalSparkProgram.PROGRAM_ARGS;
    if (sec.getRuntimeArguments().containsKey(programArgsKey)) {
      programArgs = sec.getRuntimeArguments().get(programArgsKey);
    }
    return programArgs;
  }
}
