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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.InvalidEntry;
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
import org.apache.commons.validator.GenericValidator;
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
    "and returns as a result a Map<String, String>, " +
    "Example response : " +
    "   {isValid : false, errorCode : 10, errorMsg : \"unidentified record\"} " +
    "Validation script example: " +
    "   function isValid(input) { " +
    "      var isValid = true; " +
    "      var errMsg = \"\";" +
    "      var errCode = 0;" +
    "      var resultMap = new java.util.HashMap();" +
    "      input = JSON.parse(input);" +
    "      if (!coreValidator.isDate(input.date)) { " +
    "         isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;" +
    "      } else if (!coreValidator.isUrl(input.url)) { " +
    "         isValid = false; errMsg = \"invalid url\"; errCode = 7;" +
    "      } else if (!coreValidator.isInRange(input.content_length, 0, 1024 * 1024)) {" +
    "         isValid = false; errMsg = \"content length >1MB\"; errCode = 10;" +
    "      }" +
    "      resultMap.put(\"isValid\", isValid.toString()); " +
    "      resultMap.put(\"errorCode\", errCode); " +
    "      resultMap.put(\"errorMsg\", errMsg); " +
    "      return resultMap;" +
    "   };" +
    "The isValid function in this javascript example uses core validation functions";

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(ValidatorTransform.class);

  private final ValidatorConfig config;
  private Metrics metrics;
  private Invocable invocable;

  // for unit tests, otherwise config is injected by plugin framework.
  public ValidatorTransform(ValidatorConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    for (String validatorName : config.validators.split("\\s*,\\s*")) {
      pipelineConfigurer.usePluginClass("validator", validatorName, validatorName,
                                        PluginProperties.builder().build());
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    List<Validator> validators = new ArrayList<>();
    for (String pluginId : config.validators.split("\\s*,\\s*")) {
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
    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("JavaScript");
    String scriptStr = config.validationScript;
    Preconditions.checkArgument(!Strings.isNullOrEmpty(scriptStr), "Filter script must be specified.");

    for (Validator validator : validators) {
      engine.put(validator.getValidatorName(), validator.getValidator());
    }
    engine.eval(scriptStr);
    invocable = (Invocable) engine;
    metrics = context.getMetrics();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    try {
      Object result = invocable.invokeFunction("isValid", GSON.toJson(input));
      if (!(result instanceof Map)) {
        LOG.error("isValid function did not return a Map. Please check your script for correctness");
        return;
      }
      Map<String, String> resultMap = (Map<String, String>) result;
      Preconditions.checkState(resultMap.containsKey("isValid"),
                               "Result map returned by isValid function did not contain an entry for 'isValid'");

      Preconditions.checkState(resultMap.get("isValid").equalsIgnoreCase("true")
                                 || resultMap.get("isValid").equalsIgnoreCase("false"),
                               "'isValid' entry in Result map should be 'true' or 'false'" +
                                 "please check your script for correctness");

      if (Boolean.parseBoolean(resultMap.get("isValid"))) {
        emitter.emit(input);
      } else {
        emitter.emitError(getErrorObject(resultMap, input));
        metrics.count("filtered", 1);
        LOG.trace("Error code : {} , Error Message {}", resultMap.get("errorCode"), resultMap.get("errorMsg"));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid filter condition.", e);
    }
  }

  private InvalidEntry<StructuredRecord> getErrorObject(Map<String, String> result, StructuredRecord input) {
    Preconditions.checkState(result.containsKey("errorCode"));
    Preconditions.checkState(result.containsKey("errorMsg"));
    Preconditions.checkState(GenericValidator.isInt(result.get("errorCode")),
                             "errorCode entry in resultMap is not a valid integer. " +
                               "please check your script to make sure error-code is an integer");

    int errorCode = Integer.parseInt(result.get("errorCode"));
    return new InvalidEntry<StructuredRecord>(errorCode, result.get("errorMsg"), input);
  }

  /**
   * ValidatorConfig whose list of validators and script can be configured
   */
  public static class ValidatorConfig extends PluginConfig {
    @Description("Comma separated list of validator plugins that are used in script")
    String validators;
    @Description(SCRIPT_DESCRIPTION)
    String validationScript;
  }

}
