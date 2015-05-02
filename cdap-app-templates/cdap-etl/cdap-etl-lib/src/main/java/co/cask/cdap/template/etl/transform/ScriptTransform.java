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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.TransformContext;
import co.cask.cdap.template.etl.common.StructuredRecordSerializer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Filters records using custom javascript provided by the config.
 */
@Plugin(type = "transform")
@Name("Script")
@Description("Executes user provided Javascript in order to transform one record into another")
public class ScriptTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final String FUNCTION_NAME = "dont_name_your_function_this";
  private static final String VARIABLE_NAME = "dont_name_your_variable_this";
  private ScriptEngine engine;
  private Invocable invocable;
  private Schema schema;
  private final Config config;

  /**
   * Configuration for the script transform.
   */
  public static class Config extends PluginConfig {
    @Description("Javascript defining how to transform one record into another. The script must implement a function " +
      "called 'transform', which take as input a Json object that represents the input record, and returns " +
      "a Json object that respresents the transformed input. " +
      "For example, 'function transform(input) { input.count = input.count * 1024; return input; }' " +
      "will scale the 'count' field by 1024.")
    private final String script;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    private final String schema;

    public Config(String script, String schema) {
      this.script = script;
      this.schema = schema;
    }
  }

  // for unit tests, otherwise config is injected by plugin framework.
  public ScriptTransform(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) {
    ScriptEngineManager manager = new ScriptEngineManager();
    engine = manager.getEngineByName("JavaScript");
    try {
      // this is pretty ugly, but doing this so that we can pass the 'input' json into the transform function.
      // that is, we want people to implement
      // function transform(input) { ... }
      // rather than function transform() { ... } and have them access a global variable in the function
      String script = String.format("function %s() { return transform(%s); }\n%s",
                                    FUNCTION_NAME, VARIABLE_NAME, config.script);
      engine.eval(script);
    } catch (ScriptException e) {
      throw new IllegalArgumentException("Invalid script.", e);
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
        return decodeArray((List) object, schema.getComponentSchema());
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

  @SuppressWarnings("RedundantCast")
  private Object decodeSimpleType(Object object, Schema schema) {
    Schema.Type type = schema.getType();
    switch (type) {
      case NULL:
        return null;
      // numbers come back as doubles
      case INT:
        return ((Double) object).intValue();
      case LONG:
        return ((Double) object).longValue();
      case FLOAT:
        return ((Double) object).floatValue();
      case BYTES:
        List byteArr = (List) object;
        byte[] output = new byte[byteArr.size()];
        for (int i = 0; i < output.length; i++) {
          // everything is a double
          output[i] = ((Double) byteArr.get(i)).byteValue();
        }
        return output;
      case DOUBLE:
        // case so that if it's not really a double it will fail. This is possible for unions,
        // where we don't know what the actual type of the object should be.
        return (Double) object;
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
}
