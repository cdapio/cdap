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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.api.TransformStage;
import co.cask.cdap.templates.etl.common.StructuredRecordSerializer;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Filters records using custom javascript provided by the config.
 */
public class ScriptFilterTransform extends TransformStage<StructuredRecord, StructuredRecord> {
  private static final String SCRIPT = "script";
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private ScriptEngine engine;
  private Invocable invocable;
  private Metrics metrics;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(getClass().getSimpleName());
    configurer.addProperty(new Property(
      SCRIPT,
      "Script that returns true if the input record should be filtered, and false if not. " +
        "The script has access to the input record through a variable named 'input', " +
        "which is a Json object representation of the record. " +
        "For example, 'return input.count > 100' will filter out any records whose count field is greater than 100.",
      true
    ));
  }

  @Override
  public void initialize(StageContext context) {
    ScriptEngineManager manager = new ScriptEngineManager();
    engine = manager.getEngineByName("JavaScript");
    String scriptStr = context.getPluginProperties().getProperties().get(SCRIPT);
    Preconditions.checkArgument(scriptStr != null && !scriptStr.isEmpty(), "Filter script must be specified.");

    String script = "function shouldFilter() { " + scriptStr + " }";
    try {
      engine.eval(script);
    } catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid script.", e);
    }
    invocable = (Invocable) engine;
    metrics = context.getMetrics();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    try {
      engine.eval("var input = " + GSON.toJson(input) + "; ");
      Boolean shouldFilter = (Boolean) invocable.invokeFunction("shouldFilter");
      if (!shouldFilter) {
        emitter.emit(input);
      } else {
        metrics.count("filtered", 1);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid filter condition.", e);
    }
  }
}
