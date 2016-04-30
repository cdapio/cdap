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
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class represents schema of data types.
 */
@Beta
public final class Schema implements Serializable {
  private static final SchemaTypeAdapter SCHEMA_TYPE_ADAPTER = new SchemaTypeAdapter();
  private static final long serialVersionUID = -1891891892562027345L;

  /**
   * Types known to Schema.
   */
  public enum Type {
    NULL(true),
    BOOLEAN(true),
    INT(true),
    LONG(true),
    FLOAT(true),
    DOUBLE(true),
    BYTES(true),
    STRING(true),

    ENUM(false),

    ARRAY(false),
    MAP(false),
    RECORD(false),
    UNION(false);

    private final boolean simpleType;

    Type(boolean primitive) {
      this.simpleType = primitive;
    }

    /**
     * @return true if this enum represents a simple schema type.
     */
    public boolean isSimpleType() {
      return simpleType;
    }
  }

  /**
   * Represents a field inside a {@link Type#RECORD} schema.
   */
  public static final class Field implements Serializable {
    private static final long serialVersionUID = 5423721270457378454L;
    private final String name;
    private final Schema schema;

    /**
     * Creates a {@link Field} instance with the given name and {@link Schema}.
     *
     * @param name Name of the field.
     * @param schema Schema of the field.
     * @return A new {@link Field} instance.
     */
    public static Field of(String name, Schema schema) {
      return new Field(name, schema);
    }

    private Field(String name, Schema schema) {
      this.name = name;
      this.schema = schema;
    }

    /**
     * @return Name of the field.
     */
    public String getName() {
      return name;
    }

    /**
     * @return Schema of the field.
     */
    public Schema getSchema() {
      return schema;
    }

    @Override
    public String toString() {
      return String.format("{name: %s, schema: %s}", name, schema);
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
   * Creates a {@link Schema} for the given type. The type given must be a
   * {@link Schema.Type#isSimpleType() Simple Type}.
   *
   * @param type Type of the schema to create.
   * @return A {@link Schema} with the given type.
   */
  public static Schema of(Type type) {
    if (!type.isSimpleType()) {
      throw new IllegalArgumentException("Type " + type + " is not a simple type.");
    }
    return new Schema(type, null, null, null, null, null, null, null);
  }

  /**
   * Creates a nullable {@link Schema} for the given schema, which is a union of the given schema with the null type.
   * The type given must not be the null type.
   *
   * @param schema Schema to union with the null type.
   * @return A nullable version of the given {@link Schema}.
   */
  public static Schema nullableOf(Schema schema) {
    if (schema.type == Type.NULL) {
      throw new IllegalArgumentException("Given schema must not be the null type.");
    }
    return Schema.unionOf(schema, Schema.of(Type.NULL));
  }

  /**
   * Creates a {@link Schema} of {@link Type#ENUM ENUM} type, with the given enum values.
   * The set of values given should be unique and must contains at least one value.
   * The ordering of values in the enum type schema would be the same as the order being passed in.
   *
   * @param values Enum values.
   * @return A {@link Schema} of {@link Type#ENUM ENUM} type.
   */
  public static Schema enumWith(String...values) {
    return enumWith(Arrays.asList(values));
  }

  /**
   * Creates a {@link Schema} of {@link Type#ENUM ENUM} type, with the given enum values.
   * The set of values given should be unique and must contains at least one value.
   * The ordering of values in the enum type schema would be the same as the {@link Iterable#iterator()} order.
   *
   * @param values Enum values.
   * @return A {@link Schema} of {@link Type#ENUM ENUM} type.
   */
  public static Schema enumWith(Iterable<String> values) {
    Set<String> uniqueValues = new LinkedHashSet<>();
    for (String value : values) {
      if (!uniqueValues.add(value)) {
        throw new IllegalArgumentException("Duplicate enum value is not allowed.");
      }
    }
    if (uniqueValues.isEmpty()) {
      throw new IllegalArgumentException("No enum value provided.");
    }
    return new Schema(Type.ENUM, uniqueValues, null, null, null, null, null, null);
  }

  /**
   * Creates a {@link Schema} of {@link Type#ENUM ENUM} type, with values extracted from the given {@link Enum} class.
   * The ordering of values in the enum type schema would be the same as the {@link Enum#ordinal()} order.
   *
   * @param enumClass Enum values.
   * @return A {@link Schema} of {@link Type#ENUM ENUM} type.
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
   * Creates an {@link Type#ARRAY ARRAY} {@link Schema} of the given component type.
   * @param componentSchema Schema of the array component.
   * @return A {@link Schema} of {@link Type#ARRAY ARRAY} type.
   */
  public static Schema arrayOf(Schema componentSchema) {
    return new Schema(Type.ARRAY, null, componentSchema, null, null, null, null, null);
  }

  /**
   * Creates a {@link Type#MAP MAP} {@link Schema} of the given key and value types.
   * @param keySchema Schema of the map key.
   * @param valueSchema Schema of the map value
   * @return A {@link Schema} of {@link Type#MAP MAP} type.
   */
  public static Schema mapOf(Schema keySchema, Schema valueSchema) {
    return new Schema(Type.MAP, null, null, keySchema, valueSchema, null, null, null);
  }

  /**
   * Creates a {@link Type#RECORD RECORD} {@link Schema} of the given name. The schema created
   * doesn't carry any record fields, which makes it only useful to be used as a component schema
   * for other schema type, where the actual schema is resolved from the top level container schema.
   *
   * @param name Name of the record.
   * @return A {@link Schema} of {@link Type#RECORD RECORD} type.
   */
  public static Schema recordOf(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Record name cannot be null.");
    }
    return new Schema(Type.RECORD, null, null, null, null, name, null, null);
  }

  /**
   * Creates a {@link Type#RECORD RECORD} {@link Schema} with the given name and {@link Field Fields}.
   * The ordering of the fields inside the record would be the same as the one being passed in.
   *
   * @param name Name of the record
   * @param fields All the fields that the record contains.
   * @return A {@link Schema} of {@link Type#RECORD RECORD} type.
   */
  public static Schema recordOf(String name, Field...fields) {
    return recordOf(name, Arrays.asList(fields));
  }

  /**
   * Creates a {@link Type#RECORD RECORD} {@link Schema} with the given name and {@link Field Fields}.
   * The ordering of the fields inside the record would be the same as the {@link Iterable#iterator()} order.
   *
   * @param name Name of the record
   * @param fields All the fields that the record contains.
   * @return A {@link Schema} of {@link Type#RECORD RECORD} type.
   */
  public static Schema recordOf(String name, Iterable<Field> fields) {
    if (name == null) {
      throw new IllegalArgumentException("Record name cannot be null.");
    }
    Map<String, Field> fieldMap = new LinkedHashMap<>();
    for (Field field : fields) {
      fieldMap.put(field.getName(), field);
    }
    if (fieldMap.isEmpty()) {
      throw new IllegalArgumentException("No record field provided for " + name);
    }
    return new Schema(Type.RECORD, null, null, null, null, name, fieldMap, null);
  }

  /**
   * Creates a {@link Type#UNION UNION} {@link Schema} which represents a union of all the given schemas.
   * The ordering of the schemas inside the union would be the same as the one being passed in.
   *
   * @param schemas All the {@link Schema Schemas} constitutes the union.
   * @return A {@link Schema} of {@link Type#UNION UNION} type.
   */
  public static Schema unionOf(Schema...schemas) {
    return unionOf(Arrays.asList(schemas));
  }

  /**
   * Creates a {@link Type#UNION UNION} {@link Schema} which represents a union of all the given schemas.
   * The ordering of the schemas inside the union would be the same as the {@link Iterable#iterator()} order.
   *
   * @param schemas All the {@link Schema Schemas} constitutes the union.
   * @return A {@link Schema} of {@link Type#UNION UNION} type.
   */
  public static Schema unionOf(Iterable<Schema> schemas) {
    List<Schema> schemaList = new ArrayList<>();
    for (Schema schema : schemas) {
      schemaList.add(schema);
    }
    if (schemaList.isEmpty()) {
      throw new IllegalArgumentException("No union schema provided.");
    }
    return new Schema(Type.UNION, null, null, null, null, null, null, schemaList);
  }

  private final Type type;

  private final Map<String, Integer> enumValues;
  private final Map<Integer, String> enumIndexes;

  private final Schema componentSchema;

  private final Schema keySchema;
  private final Schema valueSchema;
  private final Map.Entry<Schema, Schema> mapSchema;

  private final String recordName;
  private final Map<String, Field> fieldMap;
  private final List<Field> fields;

  private final List<Schema> unionSchemas;

  // No need to serialize the schemaString to save space
  // It can be recomputed on demand (and usually it is not used in the context that serialization is used)
  private transient String schemaString;
  private SchemaHash schemaHash;

  private Schema(Type type, Set<String> enumValues, Schema componentSchema, Schema keySchema, Schema valueSchema,
                 String recordName, Map<String, Field> fieldMap, List<Schema> unionSchemas) {
    this.type = type;
    Map.Entry<Map<String, Integer>, Map<Integer, String>> enumValuesIndexes = createIndex(enumValues);
    this.enumValues = enumValuesIndexes.getKey();
    this.enumIndexes = enumValuesIndexes.getValue();
    this.componentSchema = componentSchema;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.mapSchema = (keySchema == null || valueSchema == null) ? null : new ImmutableEntry<>(keySchema, valueSchema);
    this.recordName = recordName;
    this.fieldMap = populateRecordFields(fieldMap);
    this.fields = this.fieldMap == null ? null : Collections.unmodifiableList(new ArrayList<>(this.fieldMap.values()));
    this.unionSchemas = Collections.unmodifiableList(unionSchemas == null ? new ArrayList<Schema>()
                                                                          : new ArrayList<>(unionSchemas));
  }

  /**
   * @return The {@link Type} that this schema represents.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return An immutable {@link Set} of enum values or {@code null} if this is not a {@link Type#ENUM ENUM} schema.
   *         The {@link Set#iterator()} order would be the enum values orders.
   */
  public Set<String> getEnumValues() {
    return enumValues.keySet();
  }

  /**
   * @param value The enum value
   * @return The 0-base index of the given value in the enum values or {@code -1} if this is not a
   *         {@link Type#ENUM ENUM} schema.
   */
  public int getEnumIndex(String value) {
    if (enumValues == null) {
      return -1;
    }
    Integer idx = enumValues.get(value);
    return idx == null ? -1 : idx;
  }

  /**
   * @param idx The index in the enum values
   * @return The string represents the enum value, or {@code null} if this is not a {@link Type#ENUM ENUM} schema or
   *         the given index is invalid.
   */
  public String getEnumValue(int idx) {
    if (enumIndexes == null) {
      return null;
    }
    return enumIndexes.get(idx);
  }

  /**
   * @return The schema of the array component or {@code null} if this is not a {@link Type#ARRAY ARRAY} schema.
   */
  public Schema getComponentSchema() {
    return componentSchema;
  }

  /**
   * @return An immutable {@code Map.Entry} if this is a {@code Type#MAP MAP} schema or {@code null} otherwise.
   *         The {@code Map.Entry#getKey()} would returns the key schema, while {@code Map.Entry#getValue()}
   *         would returns the value schema.
   */
  public Map.Entry<Schema, Schema> getMapSchema() {
    return mapSchema;
  }

  /**
   * @return Name of the record if this is a {@link Type#RECORD RECORD} schema or {@code null} otherwise.
   */
  public String getRecordName() {
    return recordName;
  }

  /**
   * @return An immutable {@link List} of record {@link Field Fields} if this is a {@link Type#RECORD RECORD} schema
   *         or {@code null} otherwise.
   */
  public List<Field> getFields() {
    return fields;
  }

  /**
   * Returns the record {@link Field} of the given name.
   *
   * @param name Name of the field
   * @return A {@link Field} or {@code null} if there is no such field in this record
   *         or this is not a {@link Type#RECORD RECORD} schema.
   */
  public Field getField(String name) {
    if (fieldMap == null) {
      return null;
    }
    return fieldMap.get(name);
  }

  /**
   * @return An immutable {@link List} of schemas inside this union
   *         or {@code null} if this is not a {@link Type#UNION UNION} schema.
   */
  public List<Schema> getUnionSchemas() {
    return unionSchemas;
  }

  /**
   * @param idx Index to the union schemas
   * @return A {@link Schema} of the given union index or {@code null} if this is not a {@link Type#UNION UNION}
   *         schema or the given index is invalid.
   */
  public Schema getUnionSchema(int idx) {
    return (unionSchemas == null || idx < 0 || unionSchemas.size() <= idx) ? null : unionSchemas.get(idx);
  }

  /**
   * @return a JSON representation of this schema, which can be parsed with {@link #parseJson}
   */
  @Override
  public String toString() {
    // The follow logic is thread safe, as all the fields buildString() needs are immutable.
    // It's possible that buildString() get triggered multiple times, but they should yield the same result.
    String str = schemaString;
    if (str == null) {
      schemaString = str = buildString();
    }
    return str;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    return getSchemaHash().equals(((Schema) other).getSchemaHash());
  }

  @Override
  public int hashCode() {
    return getSchemaHash().hashCode();
  }

  /**
   * @return A MD5 hash of this schema.
   */
  public SchemaHash getSchemaHash() {
    SchemaHash hash = schemaHash;
    if (hash == null) {
      schemaHash = hash = new SchemaHash(this);
    }
    return hash;
  }


  /**
   * Checks if the given target schema is compatible with this schema, meaning datum being written with this
   * schema could be projected correctly into the given target schema.
   *
   * TODO: Add link to document of the target type projection.
   *
   * @param target Schema to check for compatibility to this target
   * @return {@code true} if the schemas are compatible, {@code false} otherwise.
   */
  public boolean isCompatible(Schema target) {
    if (equals(target)) {
      return true;
    }
    Set<Map.Entry<String, String>> recordCompared = new HashSet<>();
    return checkCompatible(target, recordCompared);
  }

  /**
   * Check if this is a nullable type, which is a union of a null and one other non-null type.
   *
   * @return true if it is a nullable type, false if not.
   */
  public boolean isNullable() {
    if ((type == Type.UNION) && (unionSchemas.size() == 2)) {
      Type type1 = unionSchemas.get(0).getType();
      Type type2 = unionSchemas.get(1).getType();
      // may not need to check that both are not null, but better to be safe
      return (type1 == Type.NULL && type2 != Type.NULL) || (type1 != Type.NULL && type2 == Type.NULL);
    }
    return false;
  }

  /**
   * Check if this is a nullable simple type, which is a union of a null and one other non-null simple type, where
   * a simple type is a boolean, int, long, float, double, bytes, or string type.
   *
   * @return whether or not this is a nullable simple type.
   */
  public boolean isNullableSimple() {
    if ((type == Type.UNION) && (unionSchemas.size() == 2)) {
      Type type1 = unionSchemas.get(0).getType();
      Type type2 = unionSchemas.get(1).getType();
      if (type1 == Type.NULL) {
        return type2 != Type.NULL && type2.isSimpleType();
      } else if (type2 == Type.NULL) {
        return type1.isSimpleType();
      }
    }
    return false;
  }

  /**
   * Check if this is a simple type or a nullable simple type, which is a union of a null and one other non-null
   * simple type, where a simple type is a boolean, int, long, float, double, bytes, or string type.
   *
   * @return whether or not this is a nullable simple type.
   */
  public boolean isSimpleOrNullableSimple() {
    return type.isSimpleType() || isNullableSimple();
  }

  /**
   * Assuming this is a union of a null and one non-null type, return the non-null schema.
   *
   * @return non-null schema from a union of a null and non-null schema.
   */
  public Schema getNonNullable() {
    Schema firstSchema = unionSchemas.get(0);
    return firstSchema.getType() == Type.NULL ? unionSchemas.get(1) : firstSchema;
  }

  private boolean checkCompatible(Schema target, Set<Map.Entry<String, String>> recordCompared) {
    if (type.isSimpleType()) {
      if (type == target.getType()) {
        // Same simple type are always compatible
        return true;
      }
      switch (target.getType()) {
        case LONG:
          return type == Type.INT;
        case FLOAT:
          return type == Type.INT || type == Type.LONG;
        case DOUBLE:
          return type == Type.INT || type == Type.LONG || type == Type.FLOAT;
        case STRING:
          return type != Type.NULL && type != Type.BYTES;
        case UNION:
          for (Schema targetSchema : target.unionSchemas) {
            if (checkCompatible(targetSchema, recordCompared)) {
              return true;
            }
          }
      }
      return false;
    }

    if (type == target.type) {
      switch (type) {
        case ENUM:
          return target.getEnumValues().containsAll(getEnumValues());
        case ARRAY:
          // The component schema must be compatible
          return componentSchema.checkCompatible(target.getComponentSchema(), recordCompared);
        case MAP:
          // Both key and value schemas must be compatible
          return keySchema.checkCompatible(target.keySchema, recordCompared)
            && valueSchema.checkCompatible(target.valueSchema, recordCompared);
        case RECORD:
          // For every common field (by name), their schema must be compatible
          if (recordCompared.add(new ImmutableEntry<>(recordName, target.recordName))) {
            for (Field field : fields) {
              Field targetField = target.getField(field.getName());
              if (targetField == null) {
                continue;
              }
              if (!field.getSchema().checkCompatible(targetField.getSchema(), recordCompared)) {
                return false;
              }
            }
          }
          return true;
        case UNION:
          // Compare each source union to target union
          for (Schema sourceSchema : unionSchemas) {
            for (Schema targetSchema : target.unionSchemas) {
              if (sourceSchema.checkCompatible(targetSchema, recordCompared)) {
                return true;
              }
            }
          }
          return false;
      }
    }

    if (type == Type.UNION || target.type == Type.UNION) {
      List<Schema> unions = type == Type.UNION ? unionSchemas : target.unionSchemas;
      Schema checkSchema = type == Type.UNION ? target : this;
      for (Schema schema : unions) {
        if (schema.checkCompatible(checkSchema, recordCompared)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Creates a pair of Maps such that the first map contains indexes based on the iteration order of the given set.
   * The second map is the inverse of the first map.
   *
   * @param values Set of values to create index on
   * @return A {@link Map.Entry} containing two maps.
   */
  private <V> Map.Entry<Map<V, Integer>, Map<Integer, V>> createIndex(Set<V> values) {
    if (values == null) {
      return new ImmutableEntry<>(null, null);
    }

    Map<V, Integer> forwardMap = new LinkedHashMap<>();
    Map<Integer, V> reverseMap = new LinkedHashMap<>();
    int idx = 0;
    for (V value : values) {
      forwardMap.put(value, idx);
      reverseMap.put(idx, value);
      idx++;
    }

    return new ImmutableEntry<>(Collections.unmodifiableMap(forwardMap), Collections.unmodifiableMap(reverseMap));
  }

  /**
   * Resolves all field schemas.
   *
   * @param fields All the fields that need to be resolved.
   * @return A {@link Map} which has all the field schemas resolved.
   * @see #resolveSchema(Schema, java.util.Map)
   */
  private Map<String, Field> populateRecordFields(Map<String, Field> fields) {
    if (fields == null) {
      return null;
    }

    Map<String, Schema> knownRecordSchemas = new HashMap<>();
    knownRecordSchemas.put(recordName, this);
    Map<String, Field> resolvedFields = new LinkedHashMap<>();

    for (Map.Entry<String, Field> fieldEntry : fields.entrySet()) {
      String fieldName = fieldEntry.getKey();
      Field field = fieldEntry.getValue();
      Schema fieldSchema = resolveSchema(field.getSchema(), knownRecordSchemas);

      if (fieldSchema == field.getSchema()) {
        resolvedFields.put(fieldName, field);
      } else {
        resolvedFields.put(fieldName, Field.of(fieldName, fieldSchema));
      }
    }

    return Collections.unmodifiableMap(resolvedFields);
  }

  /**
   * This method is to recursively resolves all name only record schema in the given schema.
   *
   * @param schema The schema needs to be resolved.
   * @param knownRecordSchemas The mapping of the already resolved record schemas.
   * @return A {@link Schema} that is structurally the same as the input schema, but with all
   *         name only record schemas resolved to full schemas (i.e. with fields sets).
   *         If nothing in the given schema needs to be resolved, the same schema instance would be returned,
   *         otherwise, a new instance would be returned.
   */
  private Schema resolveSchema(final Schema schema, final Map<String, Schema> knownRecordSchemas) {
    switch (schema.getType()) {
      case ARRAY:
        Schema componentSchema = resolveSchema(schema.getComponentSchema(), knownRecordSchemas);
        return (componentSchema == schema.getComponentSchema()) ? schema : Schema.arrayOf(componentSchema);
      case MAP:
        Map.Entry<Schema, Schema> entry = schema.getMapSchema();
        Schema keySchema = resolveSchema(entry.getKey(), knownRecordSchemas);
        Schema valueSchema = resolveSchema(entry.getValue(), knownRecordSchemas);
        return (keySchema == entry.getKey() && valueSchema == entry.getValue()) ?
                schema : Schema.mapOf(keySchema, valueSchema);
      case UNION:
        List<Schema> schemas = new ArrayList<>();
        boolean changed = false;
        for (Schema input : schema.getUnionSchemas()) {
          Schema output = resolveSchema(input, knownRecordSchemas);
          if (output != input) {
            changed = true;
          }
          schemas.add(output);
        }
        return changed ? Schema.unionOf(schemas) : schema;
      case RECORD:
        if (schema.fields == null) {
          // It is a named record that refers to previously defined record
          Schema knownSchema = knownRecordSchemas.get(schema.recordName);
          if (knownSchema == null) {
            throw new IllegalArgumentException("Undefined schema " + schema.recordName);
          }
          return knownSchema;
        } else {
          // It is a concrete schema
          knownRecordSchemas.put(schema.recordName, schema);
          return schema;
        }
    }
    return schema;
  }

  /**
   * Helper method to encode this schema into json string.
   *
   * @return A json string representing this schema.
   */
  private String buildString() {
    if (type.isSimpleType()) {
      return '"' + type.name().toLowerCase() + '"';
    }

    StringWriter writer = new StringWriter();
    try (JsonWriter jsonWriter = new JsonWriter(writer)) {
      SCHEMA_TYPE_ADAPTER.write(jsonWriter, this);
    } catch (IOException e) {
      // It should never throw IOException on the StringWriter, if it does, something very wrong.
      throw new RuntimeException(e);
    }
    return writer.toString();
  }

  private static final class ImmutableEntry<K, V> implements Map.Entry<K, V>, Serializable {

    private final K key;
    private final V value;

    private ImmutableEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException("Mutation to entry not supported");
    }

    @Override
    public String toString() {
      return getKey() + "=" + getValue();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof Map.Entry)) {
        return false;
      }
      Map.Entry other = (Map.Entry) obj;
      return Objects.equals(key, other.getKey()) && Objects.equals(value, other.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getKey(), getValue());
    }
  }
}
