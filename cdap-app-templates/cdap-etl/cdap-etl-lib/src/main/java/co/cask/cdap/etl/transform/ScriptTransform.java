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
import co.cask.cdap.api.data.schema.Schema;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Transforms records using custom javascript provided by the config.
 */
@Plugin(type = "transform")
@Name("Script")
@Description("Executes user-provided Javascript that transforms one record into another.")
public class ScriptTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final String FUNCTION_NAME = "dont_name_your_function_this";
  private static final String VARIABLE_NAME = "dont_name_your_variable_this";
  private static final String CONTEXT_NAME = "dont_name_your_context_this";
  private ScriptEngine engine;
  private Invocable invocable;
  private Schema schema;
  private final Config config;
  private StageMetrics metrics;
  private Logger logger;

  @Nullable
  private Method somValuesMethod;

  /**
   * Configuration for the script transform.
   */
  public static class Config extends PluginConfig {
    @Description("Javascript defining how to transform one record into another. The script must implement a function " +
      "called 'transform', which takes as input a JSON object (representing the input record) " +
      "and a context object (which contains CDAP metrics and logger), and returns " +
      "a JSON object that represents the transformed input. " +
      "For example:\n" +
      "'function transform(input, context) {\n" +
      "  if(input.count < 0) {\n" +
      "    context.getMetrics().count(\"negative.count\", 1);\n" +
      "    context.getLogger().debug(\"Received record with negative count\");\n" +
      "  }\n" +
      "  input.count = input.count * 1024;\n" +
      "return input; }'\n" +
      "will scale the 'count' field by 1024.")
    private final String script;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    private final String schema;

    @Description("Lookup tables to use during transform. Currently supports KeyValueTable.")
    @Nullable
    private final String lookup;

    public Config(String script, String schema, LookupConfig lookup) {
      this.script = script;
      this.schema = schema;
      this.lookup = GSON.toJson(lookup);
    }
  }

  // for unit tests, otherwise config is injected by plugin framework.
  public ScriptTransform(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // try evaluating the script to fail application creation if the script is invalid
    init(null);

    // TODO: CDAP-4169 verify existence of configured lookup tables
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    metrics = context.getMetrics();
    logger = LoggerFactory.getLogger(ScriptTransform.class.getName() + " - Stage:" + context.getStageName());

    // for Nashorn (Java 8+) support -- get method to convert ScriptObjectMirror to List
    try {
      Class<?> somClass = Class.forName("jdk.nashorn.api.scripting.ScriptObjectMirror");
      somValuesMethod = somClass.getMethod("values");
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(
        "Failed to get method ScriptObjectMirror#values() for converting ScriptObjectMirror to List. " +
        "Please check your version of Nashorn is supported.", e);
    } catch (ClassNotFoundException e) {
      // Ignore -- we don't have Nashorn, so no need to handle Nashorn
    }

    init(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    try {
      engine.eval(String.format("var %s = %s;", VARIABLE_NAME, GSON.toJson(input)));
      Map scriptOutput = (Map) invocable.invokeFunction(FUNCTION_NAME);
      StructuredRecord output = decodeRecord(scriptOutput, schema == null ? input.getSchema() : schema);
      emitter.emit(output);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not transform input: " + e.getMessage(), e);
    }
  }

  private Object decode(Object object, Schema schema) {
    Schema.Type type = schema.getType();

    switch (type) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        return decodeSimpleType(object, schema);
      case ENUM:
        break;
      case ARRAY:
        return decodeArray(jsObject2List(object), schema.getComponentSchema());
      case MAP:
        Schema keySchema = schema.getMapSchema().getKey();
        Schema valSchema = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        //noinspection unchecked
        return decodeMap((Map<Object, Object>) object, keySchema, valSchema);
      case RECORD:
        return decodeRecord((Map) object, schema);
      case UNION:
        return decodeUnion(object, schema.getUnionSchemas());
    }

    throw new RuntimeException("Unable decode object with schema " + schema);
  }

  private StructuredRecord decodeRecord(Map nativeObject, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object fieldVal = nativeObject.get(fieldName);
      builder.set(fieldName, decode(fieldVal, field.getSchema()));
    }
    return builder.build();
  }

  private List jsObject2List(Object object) {
    if (somValuesMethod != null) {
      // using Nashorn (Java 8+) -- convert ScriptObjectMirror to List
      try {
        return (List) somValuesMethod.invoke(object);
      } catch (InvocationTargetException | IllegalAccessException | ClassCastException e) {
        throw new RuntimeException("Failed to convert ScriptObjectMirror to List", e);
      }
    }
    return (List) object;
  }

  @SuppressWarnings("RedundantCast")
  private Object decodeSimpleType(Object object, Schema schema) {
    Schema.Type type = schema.getType();
    switch (type) {
      case NULL:
        return null;
      // numbers come back as Numbers
      case INT:
        return ((Number) object).intValue();
      case LONG:
        return ((Number) object).longValue();
      case FLOAT:
        return ((Number) object).floatValue();
      case BYTES:
        List byteArr = jsObject2List(object);
        byte[] output = new byte[byteArr.size()];
        for (int i = 0; i < output.length; i++) {
          // everything is a number
          output[i] = ((Number) byteArr.get(i)).byteValue();
        }
        return output;
      case DOUBLE:
        // case so that if it's not really a double it will fail. This is possible for unions,
        // where we don't know what the actual type of the object should be.
        return ((Number) object).doubleValue();
      case BOOLEAN:
        return (Boolean) object;
      case STRING:
        return (String) object;
    }
    throw new RuntimeException("Unable decode object with schema " + schema);
  }

  private Map<Object, Object> decodeMap(Map<Object, Object> object, Schema keySchema, Schema valSchema) {
    Map<Object, Object> output = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : object.entrySet()) {
      output.put(decode(entry.getKey(), keySchema), decode(entry.getValue(), valSchema));
    }
    return output;
  }

  private List<Object> decodeArray(List nativeArray, Schema componentSchema) {
    List<Object> arr = Lists.newArrayListWithCapacity(nativeArray.size());
    for (Object arrObj : nativeArray) {
      arr.add(decode(arrObj, componentSchema));
    }
    return arr;
  }

  private Object decodeUnion(Object object, List<Schema> schemas) {
    for (Schema schema : schemas) {
      try {
        return decode(object, schema);
      } catch (Exception e) {
        // could be ok, just move on and try the next schema
      }
    }

    throw new RuntimeException("Unable decode union with schema " + schemas);
  }

  private void init(LookupProvider lookup) {
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
      lookupConfig = GSON.fromJson(config.lookup, LookupConfig.class);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Invalid lookup config. Expected map of string to string", e);
    }

    engine.put(CONTEXT_NAME, new ScriptContext(
      logger, metrics, lookup, lookupConfig, js));

    try {
      // this is pretty ugly, but doing this so that we can pass the 'input' json into the transform function.
      // that is, we want people to implement
      // function transform(input) { ... }
      // rather than function transform() { ... } and have them access a global variable in the function

      String script = String.format("function %s() { return transform(%s, %s); }\n%s",
        FUNCTION_NAME, VARIABLE_NAME, CONTEXT_NAME, config.script);
      engine.eval(script);
    } catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid script: " + e.getMessage(), e);
    }
    invocable = (Invocable) engine;
    if (config.schema != null) {
      try {
        schema = Schema.parseJson(config.schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
      }
    }
  }
}
