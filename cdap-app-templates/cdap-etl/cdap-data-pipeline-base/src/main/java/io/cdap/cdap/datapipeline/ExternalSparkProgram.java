/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import io.cdap.cdap.api.app.RuntimeConfigurer;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.api.spark.SparkMain;
import io.cdap.cdap.etl.batch.BatchPhaseSpec;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.DefaultMacroEvaluator;
import io.cdap.cdap.etl.proto.v2.spec.PluginSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * A Spark program that instantiates a sparkprogram plugin and executes it. The plugin is a self contained
 * spark program.
 */
public class ExternalSparkProgram extends AbstractSpark {

  public static final String STAGE_NAME = "stage.name";
  private static final Gson GSON = new Gson();
  static final String PROGRAM_ARGS = "program.args";
  private final BatchPhaseSpec phaseSpec;
  private final StageSpec stageSpec;
  private final RuntimeConfigurer runtimeConfigurer;
  private final String deployedNamespace;
  private Spark delegateSpark;

  public ExternalSparkProgram(BatchPhaseSpec phaseSpec, StageSpec stageSpec,
                              @Nullable RuntimeConfigurer runtimeConfigurer, String deployedNamespace) {
    this.phaseSpec = phaseSpec;
    this.stageSpec = stageSpec;
    this.runtimeConfigurer = runtimeConfigurer;
    this.deployedNamespace = deployedNamespace;
  }

  @Override
  protected void configure() {
    setClientResources(phaseSpec.getClientResources());
    setDriverResources(phaseSpec.getDriverResources());
    setExecutorResources(phaseSpec.getResources());

    // register the plugins at program level so that the program can be failed by the platform early in case of
    // plugin requirements not being meet
    phaseSpec.getPhase().registerPlugins(getConfigurer(), runtimeConfigurer, deployedNamespace);
    PluginSpec pluginSpec = stageSpec.getPlugin();
    PluginProperties pluginProperties = PluginProperties.builder().addAll(pluginSpec.getProperties()).build();
    // use a UUID as plugin ID so that it doesn't clash with anything. Only using the class here to
    // check which main class is needed
    // TODO: clean this up so that we only get the class once and store it in the PluginSpec instead of getting
    // it in the pipeline spec generator and here
    Object sparkPlugin = usePlugin(pluginSpec.getType(), pluginSpec.getName(),
                                   UUID.randomUUID().toString(), pluginProperties);
    if (sparkPlugin == null) {
      // should never happen, should have been checked before by the pipeline spec generator
      throw new IllegalStateException(String.format("No plugin found of type %s and name %s for stage %s",
                                                    pluginSpec.getType(), pluginSpec.getName(), STAGE_NAME));
    }

    if (Spark.class.isAssignableFrom(sparkPlugin.getClass())) {
      // TODO: Pass in a forwarding configurer so that we can capture the properties set by the plugin
      // However the usage is very limited as the plugin can always use plugin config to preserve properties
      ((Spark) sparkPlugin).configure(getConfigurer());
    } else if (SparkMain.class.isAssignableFrom(sparkPlugin.getClass())) {
      setMainClass(ScalaSparkMainWrapper.class);
    } else {
      setMainClass(JavaSparkMainWrapper.class);
    }

    setName(phaseSpec.getPhaseName());

    Map<String, String> properties = new HashMap<>();
    properties.put(STAGE_NAME, stageSpec.getName());
    properties.put(Constants.PIPELINEID, GSON.toJson(phaseSpec, BatchPhaseSpec.class));
    setProperties(properties);
  }

  @Override
  protected void initialize() throws Exception {
    SparkClientContext context = getContext();

    BatchPhaseSpec phaseSpec = GSON.fromJson(getContext().getSpecification().getProperty(Constants.PIPELINEID),
                                             BatchPhaseSpec.class);

    SparkConf sparkConf = new SparkConf();
    for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
      sparkConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
    }
    context.setSparkConf(sparkConf);

    String stageName = context.getSpecification().getProperty(STAGE_NAME);
    Class<?> externalProgramClass = context.loadPluginClass(stageName);
    // If the external program implements Spark, instantiate it and call initialize() to provide full lifecycle support
    if (Spark.class.isAssignableFrom(externalProgramClass)) {
      MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(context),
                                                                context.getLogicalStartTime(),
                                                                context, context, context.getNamespace());
      delegateSpark = context.newPluginInstance(stageName, macroEvaluator);
      if (delegateSpark instanceof AbstractSpark) {
        //noinspection unchecked
        ((AbstractSpark) delegateSpark).initialize(context);
      }
    }
  }

  @Override
  public void destroy() {
    if (delegateSpark != null) {
      if (delegateSpark instanceof AbstractSpark) {
        ((AbstractSpark) delegateSpark).destroy();
      }
    }
  }
}
