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

package io.cdap.cdap.datapipeline;

import com.google.gson.Gson;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.common.plugin.Caller;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.spark.plugin.SparkPipelinePluginContext;

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
      MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(sec),
                                                                sec.getLogicalStartTime(),
                                                                sec.getSecureStore(),
                                                                sec.getServiceDiscoverer(),
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
