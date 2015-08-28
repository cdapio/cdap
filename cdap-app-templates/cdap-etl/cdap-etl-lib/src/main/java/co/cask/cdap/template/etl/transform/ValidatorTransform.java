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

package co.cask.cdap.template.etl.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.TransformContext;
import co.cask.cdap.template.etl.api.Validator;
import co.cask.cdap.template.etl.common.StructuredRecordSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Validator transformation
 */
@Plugin(type = "transform")
@Name("Validator")
@Description("Validates a record, writing to an error dataset if the record is invalid. " +
  "Otherwise it passes the record on to the next stage.")
public class ValidatorTransform extends Transform<StructuredRecord, StructuredRecord> {

  private static final String SCRIPT_DESCRIPTION = "Javascript that must implement a function 'isValid' that " +
    "takes a JSON object representation of the input record, " +
    "and returns Map<String, String> Example : {isValid : false, errorCode : 10, errorMsg : \"unidentified record\"} " +
    "For example: " +
    "   function isValid(input) { " +
    "      var isValid = true; " +
    "      var errMsg = \"\";" +
    "      var errCode = 0;" +
    "      var resultMap = new java.util.HashMap();" +
    "      input = JSON.parse(input);" +
    "      if (!apacheValidator.isDate(input.date)) { " +
    "         isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;" +
    "      } else if (!apacheValidator.isUrl(input.url)) { " +
    "         isValid = false; errMsg = \"invalid url\"; errCode = 7;" +
    "      } else if (!apacheValidator.isInRange(input.content_length, 0, 1024 * 1024)) {" +
    "         isValid = false; errMsg = \"content length >1MB\"; errCode = 10;" +
    "      }" +
    "      resultMap.put(\"isValid\", isValid.toString()); " +
    "      resultMap.put(\"errorCode\", errCode); " +
    "      resultMap.put(\"errorMsg\", errMsg); " +
    "      return resultMap;" +
    "   };" +
    "This Javascript function in example uses apache validation functions";

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(ValidatorTransform.class);

  private ValidatorConfig config;
  private Metrics metrics;
  private Invocable invocable;

  // for unit tests, otherwise config is injected by plugin framework.
  public ValidatorTransform(ValidatorConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    for (String validatorName : config.validators.split(",")) {
      // the validtor's have only type and name, passing the properties and id as null.
      pipelineConfigurer.usePluginClass("validator", validatorName, validatorName,
                                        PluginProperties.builder().build());
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    List<Validator> validators = new ArrayList<>();
    for (String pluginId : config.validators.split(",")) {
      validators.add((Validator) context.newPluginInstance(pluginId));
    }
    try {
      setUpInitialScript(context, validators);
    }  catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid script.", e);
    }
  }

  @VisibleForTesting
  void setUpInitialScript(TransformContext context, List<Validator> validators) throws ScriptException {
    ScriptEngine engine;
    ScriptEngineManager manager = new ScriptEngineManager();
    engine = manager.getEngineByName("JavaScript");
    String scriptStr = config.validationScript;
    Preconditions.checkArgument(!Strings.isNullOrEmpty(scriptStr), "Filter script must be specified.");

    for (Validator validator : validators) {
      engine.put(validator.getValidatorVariableName(), validator.getValidator());
    }
    engine.eval(scriptStr);
    invocable = (Invocable) engine;
    metrics = context.getMetrics();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    try {
      Map<String, String> result = (Map<String, String>) invocable.invokeFunction("isValid", GSON.toJson(input));
      if (Boolean.parseBoolean(result.get("isValid"))) {
        emitter.emit(input);
      } else {
        emitter.emitError(input);
        metrics.count("filtered", 1);
        LOG.trace("Error code : {} , Error Message {}", result.get("errorCode"), result.get("errorMsg"));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid filter condition.", e);
    }
  }

  /**
   * ValidatorConfig whose list of validators and script can be configured
   */
  public static class ValidatorConfig extends PluginConfig {
    @Description("list of validator plugins that are used in script")
    String validators;
    @Description(SCRIPT_DESCRIPTION)
    String validationScript;
  }

}
