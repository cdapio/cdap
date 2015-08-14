/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.data.schema;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.internal.io.SQLSchemaParser;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility functions for {@link Schema}.
 */
@Beta
public final class Schemas {
  private static final SchemaTypeAdapter SCHEMA_TYPE_ADAPTER = new SchemaTypeAdapter();

  /**
   * Helper method to encode this schema into json string.
   *
   * @return A json string representing this schema.
   */
  public static String buildString(Schema schema) {
    if (schema.getType().isSimpleType()) {
      return '"' + schema.getType().name().toLowerCase() + '"';
    }
    StringBuilder builder = new StringBuilder();
    JsonWriter writer = new JsonWriter(CharStreams.asWriter(builder));
    try {
      SCHEMA_TYPE_ADAPTER.write(writer, schema);
      writer.close();
      return builder.toString();
    } catch (IOException e) {
      // It should never throw IOException on the StringBuilder Writer, if it does, something very wrong.
      throw Throwables.propagate(e);
    }
  }

  /**
   * Parse the given JSON representation, as returned by {@link #toString()} into a Schema object.
   *
   * @param schemaJson the json representation of the schema
   * @return the json representation parsed into a schema object
   * @throws IOException if there was an exception parsing the schema
   */
  public static Schema parseJson(String schemaJson) throws IOException {
    return SCHEMA_TYPE_ADAPTER.fromJson(schemaJson);
  }

  /**
   * Parse the given json representation of a schema object contained in a reader into a schema object.
   *
   * @param reader the reader for reading the json representation of a schema
   * @return the parsed schema object
   * @throws IOException if there was an exception parsing the schema
   */
  public static Schema parseJson(Reader reader) throws IOException {
    return SCHEMA_TYPE_ADAPTER.fromJson(reader);
  }

  /**
   * Parse the given sql-like schema representation into a schema object. Input is expected to be a comma
   * separated list of names and types, where the type is a primitive type, array, map, record, or union.
   * The input will be parsed in a case insensitive manner.
   *
   * <pre>
   * {@code
   * column_name data_type, ...
   *
   * data_type : primitive_type
   * | array_type
   * | map_type
   * | record_type
   * | union_type
   * [ not null ]
   *
   * primitive_type: boolean
   * | int
   * | long
   * | float
   * | double
   * | bytes
   * | string
   *
   * array_type: array<data_type>
   *
   * map_type: map<data_type, data_type>
   *
   * record_type: record<field_name:data_type, ...>
   *
   * union_type: union<data_type, data_type, ...>
   * }
   * </pre>
   *
   * @param schemaString the schema to parse
   * @return the parsed schema object
   * @throws IOException if there was an exception parsing the schema
   */
  public static Schema parseSQL(String schemaString) throws IOException {
    return new SQLSchemaParser(schemaString).parse();
  }

  /**
   * Creates a {@link Schemas} for the given type. The type given must be a
   * {@link Schema.Type#isSimpleType() Simple Type}.
   *
   * @param type Type of the schema to create.
   * @return A {@link Schemas} with the given type.
   */
  public static Schema of(Schema.Type type) {
    Preconditions.checkArgument(type.isSimpleType(), "Type %s is not a simple type.", type);
    return new Schema(type, null, null, null, null, null, null, null);
  }

  /**
   * Creates a nullable {@link Schemas} for the given schema, which is a union of the given schema with the null type.
   * The type given must not be the null type.
   *
   * @param schema Schema to union with the null type.
   * @return A nullable version of the given {@link Schemas}.
   */
  public static Schema nullableOf(Schema schema) {
    Preconditions.checkArgument(schema.getType() != Schema.Type.NULL, "Given schema must not be the null type.");
    return Schemas.unionOf(schema, Schemas.of(Schema.Type.NULL));
  }

  /**
   * Creates a {@link Schemas} of {@link Schema.Type#ENUM ENUM} type, with the given enum values.
   * The set of values given should be unique and must contains at least one value.
   * The ordering of values in the enum type schema would be the same as the order being passed in.
   *
   * @param values Enum values.
   * @return A {@link Schemas} of {@link Schema.Type#ENUM ENUM} type.
   */
  public static Schema enumWith(String...values) {
    return enumWith(ImmutableList.copyOf(values));
  }

  /**
   * Creates a {@link Schemas} of {@link Schema.Type#ENUM ENUM} type, with the given enum values.
   * The set of values given should be unique and must contains at least one value.
   * The ordering of values in the enum type schema would be the same as the {@link Iterable#iterator()} order.
   *
   * @param values Enum values.
   * @return A {@link Schemas} of {@link Schema.Type#ENUM ENUM} type.
   */
  public static Schema enumWith(Iterable<String> values) {
    Set<String> uniqueValues = ImmutableSet.copyOf(values);
    Preconditions.checkArgument(uniqueValues.size() > 0, "No enum value provided.");
    Preconditions.checkArgument(Iterables.size(values) == uniqueValues.size(), "Duplicate enum value is not allowed.");
    return new Schema(Schema.Type.ENUM, uniqueValues, null, null, null, null, null, null);
  }

  /**
   * Creates a {@link Schemas} of {@link Schema.Type#ENUM ENUM} type, with values extracted from the given {@link Enum} class.
   * The ordering of values in the enum type schema would be the same as the {@link Enum#ordinal()} order.
   *
   * @param enumClass Enum values.
   * @return A {@link Schemas} of {@link Schema.Type#ENUM ENUM} type.
   */
  public static Schema enumWith(Class<Enum<?>> enumClass) {
    Enum<?>[] enumConstants = enumClass.getEnumConstants();
    String[] names = new String[enumConstants.length];
    for (int i = 0; i < enumConstants.length; i++) {
      names[i] = enumConstants[i].name();
    }
    return enumWith(names);
  }

  /**
   * Creates an {@link Schema.Type#ARRAY ARRAY} {@link Schemas} of the given component type.
   * @param componentSchema Schema of the array component.
   * @return A {@link Schemas} of {@link Schema.Type#ARRAY ARRAY} type.
   */
  public static Schema arrayOf(Schema componentSchema) {
    return new Schema(Schema.Type.ARRAY, null, componentSchema, null, null, null, null, null);
  }

  /**
   * Creates a {@link Schema.Type#MAP MAP} {@link Schemas} of the given key and value types.
   * @param keySchema Schema of the map key.
   * @param valueSchema Schema of the map value
   * @return A {@link Schemas} of {@link Schema.Type#MAP MAP} type.
   */
  public static Schema mapOf(Schema keySchema, Schema valueSchema) {
    return new Schema(Schema.Type.MAP, null, null, keySchema, valueSchema, null, null, null);
  }

  /**
   * Creates a {@link Schema.Type#RECORD RECORD} {@link Schemas} of the given name. The schema created
   * doesn't carry any record fields, which makes it only useful to be used as a component schema
   * for other schema type, where the actual schema is resolved from the top level container schema.
   *
   * @param name Name of the record.
   * @return A {@link Schemas} of {@link Schema.Type#RECORD RECORD} type.
   */
  public static Schema recordOf(String name) {
    Preconditions.checkNotNull(name, "Record name cannot be null.");
    return new Schema(Schema.Type.RECORD, null, null, null, null, name, null, null);
  }

  /**
   * Creates a {@link Schema.Type#RECORD RECORD} {@link Schemas} with the given name and {@link Schema.Field Fields}.
   * The ordering of the fields inside the record would be the same as the one being passed in.
   *
   * @param name Name of the record
   * @param fields All the fields that the record contains.
   * @return A {@link Schemas} of {@link Schema.Type#RECORD RECORD} type.
   */
  public static Schema recordOf(String name, Schema.Field...fields) {
    return recordOf(name, ImmutableList.copyOf(fields));
  }

  /**
   * Creates a {@link Schema.Type#RECORD RECORD} {@link Schemas} with the given name and {@link Schema.Field Fields}.
   * The ordering of the fields inside the record would be the same as the {@link Iterable#iterator()} order.
   *
   * @param name Name of the record
   * @param fields All the fields that the record contains.
   * @return A {@link Schemas} of {@link Schema.Type#RECORD RECORD} type.
   */
  public static Schema recordOf(String name, Iterable<Schema.Field> fields) {
    Preconditions.checkNotNull(name, "Record name cannot be null.");
    ImmutableMap.Builder<String, Schema.Field> fieldMapBuilder = ImmutableMap.builder();
    for (Schema.Field field : fields) {
      fieldMapBuilder.put(field.getName(), field);
    }
    Map<String, Schema.Field> fieldMap = fieldMapBuilder.build();
    Preconditions.checkArgument(fieldMap.size() > 0, "No record field provided for %s", name);
    return new Schema(Schema.Type.RECORD, null, null, null, null, name, fieldMap, null);
  }

  /**
   * Creates a {@link Schema.Type#UNION UNION} {@link Schemas} which represents a union of all the given schemas.
   * The ordering of the schemas inside the union would be the same as the one being passed in.
   *
   * @param schemas All the {@link Schemas Schemas} constitutes the union.
   * @return A {@link Schemas} of {@link Schema.Type#UNION UNION} type.
   */
  public static Schema unionOf(Schema...schemas) {
    return unionOf(ImmutableList.copyOf(schemas));
  }

  /**
   * Creates a {@link Schema.Type#UNION UNION} {@link Schemas} which represents a union of all the given schemas.
   * The ordering of the schemas inside the union would be the same as the {@link Iterable#iterator()} order.
   *
   * @param schemas All the {@link Schemas Schemas} constitutes the union.
   * @return A {@link Schemas} of {@link Schema.Type#UNION UNION} type.
   */
  public static Schema unionOf(Iterable<Schema> schemas) {
    List<Schema> schemaList = ImmutableList.copyOf(schemas);
    Preconditions.checkArgument(schemaList.size() > 0, "No union schema provided.");
    return new Schema(Schema.Type.UNION, null, null, null, null, null, null, schemaList);
  }
}
