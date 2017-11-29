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

import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.spark.SparkMain;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.spec.PluginSpec;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A Spark program that instantiates a sparkprogram plugin and executes it. The plugin is a self contained
 * spark program.
 */
public class ExternalSparkProgram extends AbstractSpark {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalSparkProgram.class);

  public static final String STAGE_NAME = "stage.name";
  private static final Gson GSON = new Gson();
  static final String PROGRAM_ARGS = "program.args";
  private final BatchPhaseSpec phaseSpec;
  private final StageSpec stageSpec;
  private Spark delegateSpark;

  public ExternalSparkProgram(BatchPhaseSpec phaseSpec, StageSpec stageSpec) {
    this.phaseSpec = phaseSpec;
    this.stageSpec = stageSpec;
  }

  @Override
  protected void configure() {
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

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.driver.extraJavaOptions",
                  "-XX:MaxPermSize=256m " + sparkConf.get("spark.driver.extraJavaOptions", ""));
    sparkConf.set("spark.executor.extraJavaOptions",
                  "-XX:MaxPermSize=256m " + sparkConf.get("spark.executor.extraJavaOptions", ""));
    context.setSparkConf(sparkConf);

    String stageName = context.getSpecification().getProperty(STAGE_NAME);
    Class<?> externalProgramClass = context.loadPluginClass(stageName);
    // If the external program implements Spark, instantiate it and call initialize() to provide full lifecycle support
    if (Spark.class.isAssignableFrom(externalProgramClass)) {
      MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(new BasicArguments(context),
                                                                context.getLogicalStartTime(), context,
                                                                context.getNamespace());
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
