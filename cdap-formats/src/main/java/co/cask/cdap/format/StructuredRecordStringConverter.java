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

package co.cask.cdap.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility class for converting {@link StructuredRecord} to and from json.
 */
public final class StructuredRecordStringConverter {

  // Known Java type to schema type mapping
  // Doesn't have map and array as those need to use instanceof to check
  private static final Map<Class<?>, Schema.Type> TYPE_TO_SCHEMA = new IdentityHashMap<>(
    ImmutableMap.<Class<?>, Schema.Type>builder()
      .put(Boolean.class, Schema.Type.BOOLEAN)
      .put(Byte.class, Schema.Type.INT)
      .put(Short.class, Schema.Type.INT)
      .put(Integer.class, Schema.Type.INT)
      .put(Long.class, Schema.Type.LONG)
      .put(Float.class, Schema.Type.FLOAT)
      .put(Double.class, Schema.Type.DOUBLE)
      .put(String.class, Schema.Type.STRING)
      .put(ByteBuffer.class, Schema.Type.BYTES)
      .put(byte[].class, Schema.Type.BYTES)
      .put(StructuredRecord.class, Schema.Type.RECORD)
      .build()
  );

  private static final EnumMap<Schema.Type, JsonToken> SCHEMA_TO_JSON_TYPE = new EnumMap<>(
    ImmutableMap.<Schema.Type, JsonToken>builder()
      .put(Schema.Type.NULL, JsonToken.NULL)
      .put(Schema.Type.BOOLEAN, JsonToken.BOOLEAN)
      .put(Schema.Type.INT, JsonToken.NUMBER)
      .put(Schema.Type.LONG, JsonToken.NUMBER)
      .put(Schema.Type.FLOAT, JsonToken.NUMBER)
      .put(Schema.Type.DOUBLE, JsonToken.NUMBER)
      .put(Schema.Type.STRING, JsonToken.STRING)
      .put(Schema.Type.BYTES, JsonToken.BEGIN_ARRAY)
      .put(Schema.Type.ARRAY, JsonToken.BEGIN_ARRAY)
      .put(Schema.Type.MAP, JsonToken.BEGIN_OBJECT)
      .put(Schema.Type.RECORD, JsonToken.BEGIN_OBJECT)
      .build()
  );

  /**
   * Converts a {@link StructuredRecord} to a json string.
   */
  public static String toJsonString(StructuredRecord record) throws IOException {
    StringWriter strWriter = new StringWriter();
    JsonWriter writer = new JsonWriter(strWriter);
    try {
      writeJson(writer, record.getSchema(), record);
      return strWriter.toString();
    } finally {
      writer.close();
    }
  }

  /**
   * Converts a json string to a {@link StructuredRecord} based on the schema.
   */
  public static StructuredRecord fromJsonString(String json, Schema schema) throws IOException {
    JsonReader reader = new JsonReader(new StringReader(json));
    try {
      return (StructuredRecord) readJson(reader, schema);
    } finally {
      reader.close();
    }
  }

  /**
   * Converts a {@link StructuredRecord} to a delimited string.
   */
  public static String toDelimitedString(final StructuredRecord record, String delimiter) {
    return Joiner.on(delimiter).join(
      Iterables.transform(record.getSchema().getFields(), new Function<Schema.Field, String>() {
        @Override
        public String apply(Schema.Field field) {
          return record.get(field.getName()).toString();
        }
      }));
  }

  /**
   * Converts a delimited string to a {@link StructuredRecord} based on the schema.
   */
  public static StructuredRecord fromDelimitedString(String delimitedString, String delimiter, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Iterator<Schema.Field> fields = schema.getFields().iterator();

    for (String part : Splitter.on(delimiter).split(delimitedString)) {
      if (!part.isEmpty()) {
        builder.convertAndSet(fields.next().getName(), part);
      }
    }

    return builder.build();
  }

  private static Object readJson(JsonReader reader, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        reader.nextNull();
        return null;
      case BOOLEAN:
        return reader.nextBoolean();
      case INT:
        return reader.nextInt();
      case LONG:
        return reader.nextLong();
      case FLOAT:
        // Force down cast
        return (float) reader.nextDouble();
      case DOUBLE:
        return reader.nextDouble();
      case BYTES:
        return readBytes(reader);
      case STRING:
        return reader.nextString();
      case ENUM:
        // Currently there is no standard container to represent enum type
        return reader.nextString();
      case ARRAY:
        return readArray(reader, schema.getComponentSchema());
      case MAP:
        return readMap(reader, schema.getMapSchema());
      case RECORD:
        return readRecord(reader, schema);
      case UNION:
        return readUnion(reader, schema);
    }

    throw new IOException("Unsupported schema: " + schema);
  }

  private static byte[] readBytes(JsonReader reader) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(128);
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      os.write(reader.nextInt());
    }
    reader.endArray();
    return os.toByteArray();
  }

  private static List<Object> readArray(JsonReader reader, Schema elementSchema) throws IOException {
    List<Object> result = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      result.add(readJson(reader, elementSchema));
    }
    reader.endArray();
    return result;
  }

  private static Map<Object, Object> readMap(JsonReader reader,
                                             Map.Entry<Schema, Schema> mapSchema) throws IOException {
    Schema keySchema = mapSchema.getKey();
    if (!keySchema.isCompatible(Schema.of(Schema.Type.STRING))) {
      throw new IOException("Complex key type not supported: " + keySchema);
    }

    Schema valueSchema = mapSchema.getValue();
    Map<Object, Object> result = new HashMap<>();

    reader.beginObject();
    while (reader.peek() != JsonToken.END_OBJECT) {
      Object key = convertKey(reader.nextName(), keySchema.getType());
      result.put(key, readJson(reader, valueSchema));
    }
    reader.endObject();

    return result;
  }

  private static Object convertKey(String key, Schema.Type type) throws IOException {
    switch (type) {
      case STRING:
        return key;
      case BOOLEAN:
        return Boolean.valueOf(key);
      case INT:
        return Integer.valueOf(key);
      case LONG:
        return Long.valueOf(key);
      case FLOAT:
        return Float.valueOf(key);
      case DOUBLE:
        return Double.valueOf(key);
    }
    throw new IOException("Unable to convert string to type " + type);
  }

  private static StructuredRecord readRecord(JsonReader reader, Schema schema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    reader.beginObject();
    while (reader.peek() != JsonToken.END_OBJECT) {
      Schema.Field field = schema.getField(reader.nextName());
      if (field == null) {
        // Ignore unrecognized fields
        reader.skipValue();
        continue;
      }

      builder.set(field.getName(), readJson(reader, field.getSchema()));
    }
    reader.endObject();

    return builder.build();
  }

  private static Object readUnion(JsonReader reader, Schema unionSchema) throws IOException {
    JsonToken token = reader.peek();
    // Based on the token to guess the schema
    for (Schema schema : unionSchema.getUnionSchemas()) {
      if (SCHEMA_TO_JSON_TYPE.get(schema.getType()) == token) {
        return readJson(reader, schema);
      }
    }

    throw new IOException("No matching schema found for union type: " + unionSchema);
  }

  private static void writeJson(JsonWriter writer, Schema schema, Object value) throws IOException {
    switch (schema.getType()) {
      case NULL:
        writer.nullValue();
        break;
      case BOOLEAN:
        writer.value((Boolean) value);
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        writer.value((Number) value);
        break;
      case BYTES:
        writeBytes(writer, value);
        break;
      case STRING:
        writer.value((String) value);
        break;
      case ENUM:
        writer.value(((Enum) value).name());
        break;
      case ARRAY:
        writeArray(writer, schema.getComponentSchema(), value);
        break;
      case MAP:
        writeMap(writer, schema.getMapSchema(), value);
        break;
      case RECORD:
        writeRecord(writer, schema, value);
        break;
      case UNION:
        writeJson(writer, findUnionSchema(schema, value), value);
        break;
    }
  }

  private static void writeBytes(JsonWriter writer, Object value) throws IOException {
    if (value instanceof ByteBuffer) {
      writeBytes(writer, (ByteBuffer) value);
    } else if (value.getClass().isArray() && value.getClass().getComponentType().equals(byte.class)) {
      byte[] bytes = (byte[]) value;
      writeBytes(writer, bytes, 0, bytes.length);
    } else {
      throw new IOException("Expects either ByteBuffer or byte[]. Got " + value.getClass());
    }
  }

  private static void writeBytes(JsonWriter writer, ByteBuffer buffer) throws IOException {
    if (buffer.hasArray()) {
      writeBytes(writer, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    } else {
      byte[] buf = Bytes.getBytes(buffer);
      buffer.mark();
      buffer.get(buf);
      buffer.reset();
      writeBytes(writer, buf, 0, buf.length);
    }
  }

  private static void writeBytes(JsonWriter writer, byte[] bytes, int off, int len) throws IOException {
    writer.beginArray();
    for (int i = off; i < off + len; i++) {
      writer.value(bytes[i]);
    }
    writer.endArray();
  }

  private static void writeArray(JsonWriter writer, Schema elementSchema, Object value) throws IOException {
    if (!(value instanceof Collection) && !value.getClass().isArray()) {
      throw new IOException("Expects either Collection or array. Got: " + value.getClass());
    }

    writer.beginArray();
    if (value instanceof Collection) {
      for (Object element : (Collection) value) {
        writeJson(writer, elementSchema, element);
      }
    } else {
      for (int i = 0; i < Array.getLength(value); i++) {
        writeJson(writer, elementSchema, Array.get(value, i));
      }
    }
    writer.endArray();
  }

  private static void writeMap(JsonWriter writer,
                               Map.Entry<Schema, Schema> entrySchema, Object value) throws IOException {
    if (!(value instanceof Map)) {
      throw new IOException("Expects Map. Got: " + value.getClass());
    }

    Schema keySchema = entrySchema.getKey();
    if (!keySchema.isCompatible(Schema.of(Schema.Type.STRING))) {
      throw new IOException("Complex key type not supported: " + keySchema);
    }

    Schema valueSchema = entrySchema.getValue();

    writer.beginObject();
    for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
      writer.name(entry.getKey().toString());
      writeJson(writer, valueSchema, entry.getValue());
    }
    writer.endObject();
  }

  private static void writeRecord(JsonWriter writer, Schema schema, Object value) throws IOException {
    if (!(value instanceof StructuredRecord)) {
      throw new IOException("Expects StructuredRecord. Got: " + value.getClass());
    }

    StructuredRecord record = (StructuredRecord) value;
    writer.beginObject();
    for (Schema.Field field : schema.getFields()) {
      Object fieldValue = record.get(field.getName());
      if (fieldValue != null) {
        writer.name(field.getName());
        writeJson(writer, field.getSchema(), fieldValue);
      }
    }
    writer.endObject();
  }

  private static Schema findUnionSchema(Schema unionSchema, @Nullable Object value) throws IOException {
    Schema.Type type = getSchemaType(value);

    for (Schema schema : unionSchema.getUnionSchemas()) {
      // Just match the type, not matching the detail schema as it'd be too expensive.
      if (schema.getType() == type) {
        return schema;
      }
    }
    throw new IOException("Value type " + type + " not valid in union: " + unionSchema);
  }

  private static Schema.Type getSchemaType(@Nullable Object value) throws IOException {
    if (value == null) {
      return Schema.Type.NULL;
    }

    Class<?> cls = value.getClass();
    Schema.Type type = TYPE_TO_SCHEMA.get(cls);
    if (type != null) {
      return type;
    }

    if (Collection.class.isAssignableFrom(cls) || cls.isArray()) {
      return Schema.Type.ARRAY;
    }

    if (Map.class.isAssignableFrom(cls)) {
      return Schema.Type.MAP;
    }

    throw new IOException("Unsupported type found in StructuredRecord: " + cls);
  }

  private StructuredRecordStringConverter() {
    //inaccessible constructor for static class
  }
}
