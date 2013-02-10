/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Class for serialize/deserialize Schema object to/from json through {@link com.google.gson.Gson Gson}.
 * <p>
 *  Expected usage:
 *
 *  <pre>
 *    Schema schema = ...;
 *    Gson gson = new GsonBuilder()
 *                  .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
 *                  .create();
 *    String json = gson.toJson(schema);
 *
 *    Schema newSchema = gson.fromJson(json, Schema.class);
 *  </pre>
 * </p>
 */
public final class SchemaTypeAdapter extends TypeAdapter<Schema> {

  @Override
  public void write(JsonWriter writer, Schema schema) throws IOException {
    Set<String> knownRecords = Sets.newHashSet();
    write(writer, schema, knownRecords);
  }

  @Override
  public Schema read(JsonReader reader) throws IOException {
    JsonToken token = reader.peek();
    switch (token) {
      case STRING:
        // Simple type
        return Schema.of(Schema.Type.valueOf(reader.nextString().toUpperCase()));
      case BEGIN_ARRAY:
        // Union type
        return readUnion(reader);
      case BEGIN_OBJECT:
        reader.beginObject();
        String name = reader.nextName();
        if (!"type".equals(name)) {
          throw new IOException("Property \"type\" missing.");
        }
        Schema.Type schemaType = Schema.Type.valueOf(reader.nextString().toUpperCase());

        Schema schema;
        switch (schemaType) {
          case ENUM:
            schema = readEnum(reader);
            break;
          case ARRAY:
            schema = readArray(reader);
            break;
          case MAP:
            schema = readMap(reader);
            break;
          case RECORD:
            schema = readRecord(reader);
            break;
          default:
            schema = Schema.of(schemaType);
        }
        reader.endObject();
        return schema;
    }
    throw new IOException("Malformed schema input.");
  }

  /**
   * Constructs {@link Schema.Type#UNION UNION} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return A {@link Schema} of type {@link Schema.Type#UNION UNION}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readUnion(JsonReader reader) throws IOException {
    ImmutableList.Builder<Schema> unionSchemas = ImmutableList.builder();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      unionSchemas.add(read(reader));
    }
    reader.endArray();
    return Schema.unionOf(unionSchemas.build());
  }

  /**
   * Constructs {@link Schema.Type#ENUM ENUM} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return A {@link Schema} of type {@link Schema.Type#ENUM ENUM}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readEnum(JsonReader reader) throws IOException {
    if (!"symbols".equals(reader.nextName())) {
      throw new IOException("Property \"symbols\" missing for enum.");
    }
    ImmutableList.Builder<String> enumValues = ImmutableList.builder();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      enumValues.add(reader.nextString());
    }
    reader.endArray();
    return Schema.enumWith(enumValues.build());
  }

  /**
   * Constructs {@link Schema.Type#ARRAY ARRAY} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return A {@link Schema} of type {@link Schema.Type#ARRAY ARRAY}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readArray(JsonReader reader) throws IOException {
    return Schema.arrayOf(readInnerSchema(reader, "items"));
  }

  /**
   * Constructs {@link Schema.Type#MAP MAP} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return A {@link Schema} of type {@link Schema.Type#MAP MAP}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readMap(JsonReader reader) throws IOException {
    return Schema.mapOf(readInnerSchema(reader, "keys"),
                        readInnerSchema(reader, "values"));
  }

  /**
   * Constructs {@link Schema.Type#RECORD RECORD} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return A {@link Schema} of type {@link Schema.Type#RECORD RECORD}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readRecord(JsonReader reader) throws IOException {
    if (!"name".equals(reader.nextName())) {
      throw new IOException("Property \"name\" missing for record.");
    }

    String recordName = reader.nextString();
    if (reader.peek() == JsonToken.END_OBJECT) {
      // Name only record, which is a reference record.
      return Schema.recordOf(recordName);
    }

    // Read in fields schemas
    if (!"fields".equals(reader.nextName())) {
      throw new IOException("Property \"fields\" missing for record.");
    }
    ImmutableList.Builder<Schema.Field> fieldBuilder = ImmutableList.builder();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      reader.beginObject();
      if (!"name".equals(reader.nextName())) {
        throw new IOException("Property \"name\" missing for record field.");
      }
      String fieldName = reader.nextString();
      fieldBuilder.add(Schema.Field.of(fieldName, readInnerSchema(reader, "type")));
      reader.endObject();
    }
    reader.endArray();
    return Schema.recordOf(recordName, fieldBuilder.build());
  }

  /**
   * Constructs a {@link Schema} from the "key":"schema" pair.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @param key The json property name that need to match.
   * @return A {@link Schema} object representing the schema of the json input.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readInnerSchema(JsonReader reader, String key) throws IOException {
    if (!key.equals(reader.nextName())) {
      throw new IOException("Property \"" + key + "\" missing.");
    }
    return read(reader);
  }

  /**
   * Writes the given {@link Schema} into json.
   *
   * @param writer A {@link JsonWriter} for emitting json.
   * @param schema The {@link Schema} object to encode to json.
   * @param knownRecords Set of record names that has already been encoded.
   * @return The same {@link JsonWriter} as the one passed in.
   * @throws IOException When fails to encode the schema into json.
   */
  private JsonWriter write(JsonWriter writer, Schema schema, Set<String> knownRecords) throws IOException {
    // Simple type, just emit the type name as a string
    if (schema.getType().isSimpleType()) {
      return writer.value(schema.getType().name().toLowerCase());
    }

    // Union type is an array of schemas
    if (schema.getType() == Schema.Type.UNION) {
      writer.beginArray();
      for (Schema unionSchema : schema.getUnionSchemas()) {
        write(writer, unionSchema, knownRecords);
      }
      return writer.endArray();
    }

    // Complex types, represented as an object with "type" property carrying the type name
    writer.beginObject().name("type").value(schema.getType().name().toLowerCase());
    switch (schema.getType()) {
      case ENUM:
        // Emits all enum values as an array, keyed by "symbols"
        writer.name("symbols").beginArray();
        for (String enumValue : schema.getEnumValues()) {
          writer.value(enumValue);
        }
        writer.endArray();
        break;

      case ARRAY:
        // Emits the schema of the array component type, keyed by "items"
        write(writer.name("items"), schema.getComponentSchema(), knownRecords);
        break;

      case MAP:
        // Emits schema of both key and value types, keyed by "keys" and "values" respectively
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        write(writer.name("keys"), mapSchema.getKey(), knownRecords);
        write(writer.name("values"), mapSchema.getValue(), knownRecords);
        break;

      case RECORD:
        // Emits the name of record, keyed by "name"
        writer.name("name").value(schema.getRecordName());

        // If the record hasn't been emitted before, emit all the fields as an array, keyed by "fields"
        if (!knownRecords.contains(schema.getRecordName())) {
          knownRecords.add(schema.getRecordName());
          writer.name("fields").beginArray();

          // Each field is an object, with field name keyed by "name" and field schema keyed by "type"
          for (Schema.Field field : schema.getFields()) {
            writer.beginObject().name("name").value(field.getName());
            write(writer.name("type"), field.getSchema(), knownRecords);
            writer.endObject();
          }
          writer.endArray();
        }
        break;
    }
    writer.endObject();

    return writer;
  }
}
