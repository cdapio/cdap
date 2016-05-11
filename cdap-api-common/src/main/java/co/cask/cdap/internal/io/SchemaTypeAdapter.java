/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String SYMBOLS = "symbols";
  private static final String ITEMS = "items";
  private static final String KEYS = "keys";
  private static final String VALUES = "values";
  private static final String FIELDS = "fields";

  @Override
  public void write(JsonWriter writer, Schema schema) throws IOException {
    if (schema == null) {
      writer.nullValue();
      return;
    }
    Set<String> knownRecords = new HashSet<>();
    write(writer, schema, knownRecords);
  }

  @Override
  public Schema read(JsonReader reader) throws IOException {
    return read(reader, new HashMap<String, Schema>());
  }

  /**
   * Reads json value and convert it into {@link Schema} object.
   *
   * @param reader Source of json
   * @param knownRecords Set of record name already encountered during the reading.
   * @return A {@link Schema} reflecting the json.
   * @throws IOException Any error during reading.
   */
  private Schema read(JsonReader reader, Map<String, Schema> knownRecords) throws IOException {
    JsonToken token = reader.peek();
    switch (token) {
      case NULL:
        return null;
      case STRING: {
        // Simple type or know record type
        String name = reader.nextString();
        if (knownRecords.containsKey(name)) {
          Schema schema = knownRecords.get(name);
          /*
             schema is null and in the map if this is a recursive reference. For example,
             if we're looking at the inner 'node' record in the example below:
             {
               "type": "record",
               "name": "node",
               "fields": [{
                 "name": "children",
                 "type": [{
                   "type": "array",
                   "items": ["node", "null"]
                 }, "null"]
               }, {
                 "name": "data",
                 "type": "int"
               }]
             }
           */
          return schema == null ? Schema.recordOf(name) : schema;
        }
        return Schema.of(Schema.Type.valueOf(name.toUpperCase()));
      }
      case BEGIN_ARRAY:
        // Union type
        return readUnion(reader, knownRecords);
      case BEGIN_OBJECT:
        return readObject(reader, knownRecords);
    }
    throw new IOException("Malformed schema input.");
  }

  /**
   * Read JSON object and return Schema corresponding to it.
   * @param reader JsonReader used to read the json object
   * @param knownRecords Set of record name already encountered during the reading.
   * @return Schema reflecting json
   * @throws IOException when error occurs during reading json
   */
  private Schema readObject(JsonReader reader, Map<String, Schema> knownRecords) throws IOException {
    reader.beginObject();
    // Type of the schema
    Schema.Type schemaType = null;
    // Name of the element
    String elementName = null;
    // Store enum values for ENUM type
    List<String> enumValues = new ArrayList<>();
    // Store schema for key and value for MAP type
    Schema keys = null;
    Schema values = null;
    // List of fields for RECORD type
    List<Schema.Field> fields = null;
    // List of items for ARRAY type
    Schema items = null;
    // Loop through current object and populate the fields as required
    // For ENUM type List of enumValues will be populated
    // For ARRAY type items will be populated
    // For MAP type keys and values will be populated
    // For RECORD type fields will be popuated
    while (reader.hasNext()) {
      String name = reader.nextName();
      switch (name) {
        case TYPE:
          schemaType = Schema.Type.valueOf(reader.nextString().toUpperCase());
          break;
        case NAME:
          elementName = reader.nextString();
          if (schemaType == Schema.Type.RECORD) {
            /*
              Put a null schema in the map for the recursive references.
              For example, if we are looking at the outer 'node' reference in the example below, we
              add the record name in the knownRecords map, so that when we get to the inner 'node'
              reference, we know that its a record type and not a Schema.Type.
              {
                "type": "record",
                "name": "node",
                "fields": [{
                  "name": "children",
                  "type": [{
                    "type": "array",
                    "items": ["node", "null"]
                  }, "null"]
                },
                {
                  "name": "data",
                  "type": "int"
                }]
              }
              Full schema corresponding to this RECORD will be put in knownRecords once the fields in the
              RECORD are explored.
            */
            knownRecords.put(elementName, null);
          }
          break;
        case SYMBOLS:
          enumValues = readEnum(reader);
          break;
        case ITEMS:
          items = read(reader, knownRecords);
          break;
        case KEYS:
          keys = read(reader, knownRecords);
          break;
        case VALUES:
          values = read(reader, knownRecords);
          break;
        case FIELDS:
          fields = getFields(name, reader, knownRecords);
          knownRecords.put(elementName, Schema.recordOf(elementName, fields));
          break;
        default:
          reader.skipValue();
          break;
      }
    }
    reader.endObject();
    if (schemaType == null) {
      throw new IllegalStateException("Schema type cannot be null.");
    }
    Schema schema;
    switch (schemaType) {
      case ARRAY:
        schema = Schema.arrayOf(items);
        break;
      case ENUM:
        schema = Schema.enumWith(enumValues);
        break;
      case MAP:
        schema = Schema.mapOf(keys, values);
        break;
      case RECORD:
        schema = Schema.recordOf(elementName, fields);
        break;
      default:
        schema = Schema.of(schemaType);
        break;
    }
    return schema;
  }

  /**
   * Constructs {@link Schema.Type#UNION UNION} type schema from the json input.
   *
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @param knownRecords Map of record names and associated schema already encountered during the reading
   * @return A {@link Schema} of type {@link Schema.Type#UNION UNION}.
   * @throws IOException When fails to construct a valid schema from the input.
   */
  private Schema readUnion(JsonReader reader, Map<String, Schema> knownRecords) throws IOException {
    List<Schema> unionSchemas = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      unionSchemas.add(read(reader, knownRecords));
    }
    reader.endArray();
    return Schema.unionOf(unionSchemas);
  }

  /**
   * Returns the {@link List} of enum values from the json input.
   * @param reader The {@link JsonReader} for streaming json input tokens.
   * @return a list of enum values
   * @throws IOException When fails to parse the input json.
   */
  private List<String> readEnum(JsonReader reader) throws IOException {
    List<String> enumValues = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      enumValues.add(reader.nextString());
    }
    reader.endArray();
    return enumValues;
  }

  /**
   * Get the list of {@link Schema.Field} associated with current RECORD.
   * @param recordName the name of the RECORD for which fields to be returned
   * @param reader the reader to read the record
   * @param knownRecords record names already encountered during the reading
   * @return the list of fields associated with the current record
   * @throws IOException when error occurs during reading the json
   */
  private List<Schema.Field> getFields(String recordName, JsonReader reader, Map<String, Schema> knownRecords)
    throws IOException {
    knownRecords.put(recordName, null);
    List<Schema.Field> fieldBuilder = new ArrayList<>();
    reader.beginArray();
    while (reader.peek() != JsonToken.END_ARRAY) {
      reader.beginObject();
      String fieldName = null;
      Schema innerSchema = null;

      while (reader.hasNext()) {
        String name = reader.nextName();
        switch(name) {
          case NAME:
            fieldName = reader.nextString();
            break;
          case TYPE:
            innerSchema = read(reader, knownRecords);
            break;
          default:
            reader.skipValue();
        }
      }
      fieldBuilder.add(Schema.Field.of(fieldName, innerSchema));
      reader.endObject();
    }
    reader.endArray();
    return fieldBuilder;
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

    // If it is a record that refers to a previously defined record, just emit the name of it
    if (schema.getType() == Schema.Type.RECORD && knownRecords.contains(schema.getRecordName())) {
      return writer.value(schema.getRecordName());
    }
    // Complex types, represented as an object with "type" property carrying the type name
    writer.beginObject().name(TYPE).value(schema.getType().name().toLowerCase());
    switch (schema.getType()) {
      case ENUM:
        // Emits all enum values as an array, keyed by "symbols"
        writer.name(SYMBOLS).beginArray();
        for (String enumValue : schema.getEnumValues()) {
          writer.value(enumValue);
        }
        writer.endArray();
        break;

      case ARRAY:
        // Emits the schema of the array component type, keyed by "items"
        write(writer.name(ITEMS), schema.getComponentSchema(), knownRecords);
        break;

      case MAP:
        // Emits schema of both key and value types, keyed by "keys" and "values" respectively
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        write(writer.name(KEYS), mapSchema.getKey(), knownRecords);
        write(writer.name(VALUES), mapSchema.getValue(), knownRecords);
        break;

      case RECORD:
        // Emits the name of record, keyed by "name"
        knownRecords.add(schema.getRecordName());
        writer.name(NAME).value(schema.getRecordName())
              .name(FIELDS).beginArray();
        // Each field is an object, with field name keyed by "name" and field schema keyed by "type"
        for (Schema.Field field : schema.getFields()) {
          writer.beginObject().name(NAME).value(field.getName());
          write(writer.name(TYPE), field.getSchema(), knownRecords);
          writer.endObject();
        }
        writer.endArray();
        break;
    }
    writer.endObject();

    return writer;
  }
}
