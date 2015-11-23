/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.ScriptConstants;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.common.StructuredRecordSerializer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Filters records using custom javascript provided by the config.
 */
@Plugin(type = "transform")
@Name("ScriptFilter")
@Description("A transform plugin that filters records using a custom Javascript provided in the plugin's config.")
public class ScriptFilterTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final String SCRIPT_DESCRIPTION = "Javascript that must implement a function 'shouldFilter' that " +
    "takes a JSON object representation of the input record and a context object (which encapsulates CDAP metrics " +
    "and logger) and returns true if the input record should be filtered and false if not. " +
    "For example:\n" +
    "'function shouldFilter(input, context) {\n" +
      "if (input.count < 0) {\n" +
        "context.getLogger().info(\"Got input record with negative count\");\n" +
        "context.getMetrics().count(\"negative.count\", 1);\n" +
      "}\n" +
      "return input.count > 100;\n" +
    "}\n' " +
    "will filter out any records whose 'count' field is greater than 100.";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final String FUNCTION_NAME = "dont_name_your_function_this";
  private static final String VARIABLE_NAME = "dont_name_your_variable_this";
  private static final String CONTEXT_NAME = "dont_name_your_context_this";

  private final ScriptFilterConfig scriptFilterConfig;

  private ScriptEngine engine;
  private Invocable invocable;
  private StageMetrics metrics;
  private Logger logger;

  public ScriptFilterTransform(ScriptFilterConfig scriptFilterConfig) {
    this.scriptFilterConfig = scriptFilterConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(scriptFilterConfig.script), "Filter script must be specified.");
    // try evaluating the script to fail application creation if the script is invalid
    init(null);

    // TODO: CDAP-4169 verify existence of configured lookup tables
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    metrics = context.getMetrics();
    logger = LoggerFactory.getLogger(ScriptFilterTransform.class.getName() + " - Stage:" + context.getStageName());
    init(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    try {
      engine.eval(String.format("var %s = %s;", VARIABLE_NAME, GSON.toJson(input)));
      Boolean shouldFilter = (Boolean) invocable.invokeFunction(FUNCTION_NAME);
      if (!shouldFilter) {
        emitter.emit(input);
      } else {
        metrics.count("filtered", 1);
        metrics.pipelineCount("filtered", 1);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid filter condition.", e);
    }
  }

  private void init(LookupProvider lookupProvider) {
    ScriptEngineManager manager = new ScriptEngineManager();
    engine = manager.getEngineByName("JavaScript");
    try {
      engine.eval(ScriptConstants.HELPER_DEFINITION);
    } catch (ScriptException e) {
      // shouldn't happen
      throw new IllegalStateException("Couldn't define helper functions", e);
    }

    JavaTypeConverters js = ((Invocable) engine).getInterface(
      engine.get(ScriptConstants.HELPER_NAME), JavaTypeConverters.class);

    LookupConfig lookupConfig;
    try {
      lookupConfig = GSON.fromJson(scriptFilterConfig.lookup, LookupConfig.class);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Invalid lookup config. Expected map of string to string", e);
    }

    engine.put(CONTEXT_NAME, new ScriptContext(
      logger, metrics, lookupProvider, lookupConfig, js));

    // this is pretty ugly, but doing this so that we can pass the 'input' json into the shouldFilter function.
    // that is, we want people to implement
    // function shouldFilter(input) { ... }
    // rather than function shouldFilter() { ... } and have them access a global variable in the function
    try {
      String script = String.format("function %s() { return shouldFilter(%s, %s); }\n%s",
        FUNCTION_NAME, VARIABLE_NAME, CONTEXT_NAME, scriptFilterConfig.script);
      engine.eval(script);
    } catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid script: " + e.getMessage(), e);
    }
    invocable = (Invocable) engine;
  }

  /**
   * {@link PluginConfig} class for {@link ScriptFilterTransform}
   */
  public static class ScriptFilterConfig extends PluginConfig {
    @Description(SCRIPT_DESCRIPTION)
    String script;

    @Description("Lookup tables to use during transform. Currently supports KeyValueTable.")
    @Nullable
    String lookup;
  }
}
