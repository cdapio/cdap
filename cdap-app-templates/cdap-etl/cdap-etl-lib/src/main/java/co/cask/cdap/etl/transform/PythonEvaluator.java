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
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transforms records using custom Python provided by the config.
 */
@Plugin(type = "transform")
@Name("PythonEvaluator")
@Description("Executes user-provided Python that transforms one record into another.")
public class PythonEvaluator extends Transform<StructuredRecord, StructuredRecord> {
  private static final String OUTPUT_VARIABLE_NAME = "dont_name_your_variable_this0";
  private static final String INPUT_STRUCTURED_RECORD_VARIABLE_NAME = "dont_name_your_variable_this1";
  private static final String EMITTER_VARIABLE_NAME = "dont_name_your_variable_this2";
  private static final String CONTEXT_NAME = "dont_name_your_context_this";
  private Schema schema;
  private final Config config;
  private Metrics metrics;
  private Logger logger;
  private PythonInterpreter interpreter;
  private PyCode compiledScript;

  /**
   * Configuration for the script transform.
   */
  public static class Config extends PluginConfig {
    @Description("Python defining how to transform one record into another. The script must implement a function " +
      "called 'transform', which takes as input a dictionary representing the input record " +
      "and a context object (which contains CDAP metrics and logger), and returns " +
      "a dictionary that represents the transformed input. " +
      "For example:\n" +
      "'def transform(input, emitter, context):\n" +
      "  input['count'] *= 1024\n" +
      "  if(input['count'] < 0):\n" +
      "    context.getMetrics().count(\"negative.count\", 1)\n" +
      "    context.getLogger().debug(\"Received record with negative count\")\n" +
      "  emitter.emit(input)'\n" +
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
  public PythonEvaluator(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // try evaluating the script to fail application creation if the script is invalid
    try {
      init();
    } finally {
      if (interpreter != null) {
        interpreter.cleanup();
        interpreter.getSystemState().cleanup();
      }
    }
  }

  @Override
  public void initialize(TransformContext context) {
    metrics = context.getMetrics();
    logger = LoggerFactory.getLogger(PythonEvaluator.class.getName() + " - Stage:" + context.getStageId());
    init();
  }

  @Override
  public void destroy() {
    interpreter.cleanup();
  }

  /**
   * Emitter to be used from within Python code
   */
  public final class PythonEmitter implements Emitter<Map> {

    private final Emitter<StructuredRecord> emitter;
    private final Schema schema;

    public PythonEmitter(Emitter<StructuredRecord> emitter, Schema schema) {
      this.emitter = emitter;
      this.schema = schema;
    }

    @Override
    public void emit(Map value) {
      emitter.emit(decode(value));
    }

    @Override
    public void emitError(InvalidEntry<Map> invalidEntry) {
      emitter.emitError(new InvalidEntry<>(invalidEntry.getErrorCode(), invalidEntry.getErrorMsg(),
                                           decode(invalidEntry.getInvalidRecord())));
    }

    public void emitError(Map invalidEntry) {
      emitter.emitError(new InvalidEntry<>((int) invalidEntry.get("errorCode"),
                                           (String) invalidEntry.get("errorMsg"),
                                           decode((Map) invalidEntry.get("invalidRecord"))));
    }

    private StructuredRecord decode(Map nativeObject) {
      return decodeRecord(nativeObject, schema);
    }
  }

  @Override
  public void transform(StructuredRecord input, final Emitter<StructuredRecord> emitter) {
    try {
      Emitter<Map> pythonEmitter = new PythonEmitter(emitter, schema == null ? input.getSchema() : schema);
      interpreter.set(INPUT_STRUCTURED_RECORD_VARIABLE_NAME, encode(input, input.getSchema()));
      interpreter.set(EMITTER_VARIABLE_NAME, pythonEmitter);
      Py.runCode(compiledScript, interpreter.getLocals(), interpreter.getLocals());

    } catch (Exception e) {
      throw new IllegalArgumentException("Could not transform input: " + e.getMessage(), e);
    }
  }

  private Object encode(Object object, Schema schema) {
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
        return object;
      case ENUM:
        break;
      case ARRAY:
        return encodeArray((List) object, schema.getComponentSchema());
      case MAP:
        Schema keySchema = schema.getMapSchema().getKey();
        Schema valSchema = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        //noinspection unchecked
        return encodeMap((Map<Object, Object>) object, keySchema, valSchema);
      case RECORD:
        return encodeRecord((StructuredRecord) object, schema);
      case UNION:
        return encodeUnion(object, schema.getUnionSchemas());
    }

    throw new RuntimeException("Unable decode object with schema " + schema);
  }

  private Object encodeRecord(StructuredRecord record, Schema schema) {
    Map<String, Object> map = new HashMap<>();
    for (Schema.Field field : schema.getFields()) {
      map.put(field.getName(), encode(record.get(field.getName()), field.getSchema()));
    }
    return map;
  }

  private Object encodeUnion(Object object, List<Schema> unionSchemas) {
    for (Schema schema : unionSchemas) {
      try {
        return encode(object, schema);
      } catch (Exception e) {
        // could be ok, just move on and try the next schema
      }
    }

    throw new RuntimeException("Unable decode union with schema " + unionSchemas);
  }

  private Object encodeMap(Map<Object, Object> map, Schema keySchema, Schema valSchema) {
    Map<Object, Object> encoded = new HashMap<>();
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      encoded.put(encode(entry.getKey(), keySchema), encode(entry.getValue(), valSchema));
    }
    return encoded;
  }

  private Object encodeArray(List list, Schema componentSchema) {
    List<Object> encoded = new ArrayList<>();
    for (Object object : list) {
      encoded.add(encode(object, componentSchema));
    }
    return encoded;
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
      case INT:
        return (Integer) object;
      case LONG:
        if (object instanceof BigInteger) {
          return ((BigInteger) object).longValue();
        }
        return (Long) object;
      case FLOAT:
        return (Float) object;
      case BYTES:
        return (byte[]) object;
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

  private void init() {
    interpreter = new PythonInterpreter();
    interpreter.set(CONTEXT_NAME, new ScriptContext(logger, metrics));

    // this is pretty ugly, but doing this so that we can pass the 'input' json into the transform function.
    // that is, we want people to implement
    // function transform(input) { ... }
    // rather than function transform() { ... } and have them access a global variable in the function

    String script = String.format("%s\n%s = transform(%s, %s, %s)",
                                  config.script, OUTPUT_VARIABLE_NAME, INPUT_STRUCTURED_RECORD_VARIABLE_NAME,
                                  EMITTER_VARIABLE_NAME, CONTEXT_NAME);
    compiledScript = interpreter.compile(script);
    if (config.schema != null) {
      try {
        schema = Schema.parseJson(config.schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage(), e);
      }
    }
  }
}
