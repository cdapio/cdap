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
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.api.Validator;
import co.cask.cdap.etl.common.StructuredRecordSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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
    "takes a JSON object representation of the input record " +
    "and a context object (encapsulating CDAP metrics, logger, and validators) " +
    "and returns a result JSON with validity, error code, and error message." +
    "Example response: " +
    "   {isValid : false, errorCode : 10, errorMsg : \"unidentified record\"} " +
    "Validation script example: " +
    "   function isValid(input, context) { " +
    "      var isValid = true; " +
    "      var errMsg = \"\";" +
    "      var errCode = 0;" +
    "      var coreValidator = context.getValidator(\"coreValidator\");" +
    "      var metrics = context.getMetrics();" +
    "      var logger = context.getLogger();" +
    "      if (!coreValidator.isDate(input.date)) { " +
    "         isValid = false; errMsg = input.date + \"is invalid date\"; errCode = 5;" +
    "         metrics.count(\"invalid.date\", 1);" +
    "      } else if (!coreValidator.isUrl(input.url)) { " +
    "         isValid = false; errMsg = \"invalid url\"; errCode = 7;" +
    "         metrics.count(\"invalid.url\", 1);" +
    "      } else if (!coreValidator.isInRange(input.content_length, 0, 1024 * 1024)) {" +
    "         isValid = false; errMsg = \"content length >1MB\"; errCode = 10;" +
    "         metrics.count(\"invalid.body.size\", 1);" +
    "      }" +
    "      if (!isValid) {" +
    "       logger.warn(\"Validation failed for record {}\", input);" +
    "      }" +
    "      return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
    "   };" +
    "The isValid function in this Javascript example uses CoreValidator functions.";

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(ValidatorTransform.class);
  private static final String VARIABLE_NAME = "dont_name_your_variable_this";
  private static final String FUNCTION_NAME = "dont_name_your_function_this";
  private static final String CONTEXT_NAME = "dont_name_your_context_this";

  private final ValidatorConfig config;
  private Metrics metrics;
  private Invocable invocable;
  private ScriptEngine engine;
  private Logger logger;

  // for unit tests, otherwise config is injected by plugin framework.
  public ValidatorTransform(ValidatorConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    List<Validator> validators = new ArrayList<>();
    for (String validatorName : config.validators.split("\\s*,\\s*")) {
      Validator validator =
        pipelineConfigurer.usePlugin("validator", validatorName, validatorName, PluginProperties.builder().build());
      if (validator == null) {
        throw new IllegalArgumentException("No validator plugin named " + validatorName + " could be found.");
      }
      validators.add(validator);
    }
    try {
      init(validators);
    } catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid validation script: " + e.getMessage(), e);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    List<Validator> validators = new ArrayList<>();
    for (String pluginId : config.validators.split("\\s*,\\s*")) {
      validators.add((Validator) context.newPluginInstance(pluginId));
    }
    setUpInitialScript(context, validators);
  }

  @VisibleForTesting
  void setUpInitialScript(TransformContext context, List<Validator> validators) throws ScriptException {
    metrics = context.getMetrics();
    logger = LoggerFactory.getLogger(ValidatorTransform.class.getName() + " - Stage:" + context.getStageId());
    init(validators);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    try {
      engine.eval(String.format("var %s = %s;", VARIABLE_NAME, GSON.toJson(input)));
      Map result = (Map) invocable.invokeFunction(FUNCTION_NAME);

      Preconditions.checkState(result.containsKey("isValid"),
                               "Result map returned by isValid function did not contain an entry for 'isValid'");


      if ((Boolean) result.get("isValid")) {
        emitter.emit(input);
      } else {
        emitter.emitError(getErrorObject(result, input));
        metrics.count("invalid", 1);
        LOG.trace("Error code : {} , Error Message {}", result.get("errorCode"), result.get("errorMsg"));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid filter condition.", e);
    }
  }

  private InvalidEntry<StructuredRecord> getErrorObject(Map result, StructuredRecord input) {
    Preconditions.checkState(result.containsKey("errorCode"));

    Object errorCode = result.get("errorCode");
    Preconditions.checkState(errorCode instanceof Double,
                             "errorCode entry in resultMap is not a valid number. " +
                               "please check your script to make sure error-code is a number");
    Double errorCodeNum = (Double) errorCode;
    Preconditions.checkState((errorCodeNum >= Integer.MIN_VALUE && errorCodeNum <= Integer.MAX_VALUE),
                             "errorCode must be a valid Integer");
    return new InvalidEntry<>(errorCodeNum.intValue(), (String) result.get("errorMsg"), input);
  }

  private void init(List<Validator> validators) throws ScriptException {
    ScriptEngineManager manager = new ScriptEngineManager();
    engine = manager.getEngineByName("JavaScript");
    String scriptStr = config.validationScript;
    Preconditions.checkArgument(!Strings.isNullOrEmpty(scriptStr), "Filter script must be specified.");

    Map<String, Object> validatorMap = new HashMap<>();
    for (Validator validator : validators) {
      // NOTE : This has been kept for backward compatibility, can be removed after deprecation.
      engine.put(validator.getValidatorName(), validator.getValidator());
      validatorMap.put(validator.getValidatorName(), validator.getValidator());
    }
    engine.put(CONTEXT_NAME, new ValidatorScriptContext(logger, metrics, validatorMap));

    // this is pretty ugly, but doing this so that we can pass the 'input' json into the isValid function.
    // that is, we want people to implement
    // function isValid(input) { ... }
    // rather than function isValid() { ... } with the input record assigned to the global variable
    // and have them access the global variable in the function
    String script = String.format("function %s() { return isValid(%s, %s); }\n%s",
      FUNCTION_NAME, VARIABLE_NAME, CONTEXT_NAME, config.validationScript);
    engine.eval(script);
    invocable = (Invocable) engine;
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
