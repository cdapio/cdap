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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents schema of data types.
 */
@Beta
public final class Schema {

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

    private Type(boolean primitive) {
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
  public static final class Field {
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

  private SchemaHash schemaHash;

  public Schema(Type type, Set<String> enumValues, Schema componentSchema, Schema keySchema, Schema valueSchema,
                String recordName, Map<String, Field> fieldMap, List<Schema> unionSchemas) {
    this.type = type;
    this.enumValues = createIndex(enumValues);
    this.enumIndexes = this.enumValues == null ? null : inverse(this.enumValues);
    this.componentSchema = componentSchema;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.mapSchema = (keySchema == null || valueSchema == null) ?
      null : new AbstractMap.SimpleImmutableEntry<>(keySchema, valueSchema);
    this.recordName = recordName;
    this.fieldMap = populateRecordFields(fieldMap);
    this.fields = this.fieldMap == null ? null : new ArrayList<>(this.fieldMap.values());
    this.unionSchemas = unionSchemas;
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
    return checkCompatible(target, new HashMap<String, String>());
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

  private boolean checkCompatible(Schema target, Map<String, String> recordCompared) {
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
          if (!(recordCompared.containsKey(recordName) && recordCompared.get(recordName).equals(target.recordName))) {
            recordCompared.put(recordName, target.recordName);
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
   * Creates a map of indexes based on the iteration order of the given set.
   *
   * @param values Set of values to create index on
   * @return A map from the values to indexes in the set iteration order.
   */
  private <V> Map<V, Integer> createIndex(Set<V> values) {
    if (values == null) {
      return null;
    }

    Map<V, Integer> result = new HashMap<>();
    int idx = 0;
    for (V value : values) {
      result.put(value, idx++);
    }
    return result;
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
    Map<String, Field> result = new HashMap<>();

    for (Map.Entry<String, Field> fieldEntry : fields.entrySet()) {
      String fieldName = fieldEntry.getKey();
      Field field = fieldEntry.getValue();
      Schema fieldSchema = resolveSchema(field.getSchema(), knownRecordSchemas);

      if (fieldSchema == field.getSchema()) {
        result.put(fieldName, field);
      } else {
        result.put(fieldName, Field.of(fieldName, fieldSchema));
      }
    }

    return result;
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
        return (componentSchema == schema.getComponentSchema()) ? schema : Schemas.arrayOf(componentSchema);
      case MAP:
        Map.Entry<Schema, Schema> entry = schema.getMapSchema();
        Schema keySchema = resolveSchema(entry.getKey(), knownRecordSchemas);
        Schema valueSchema = resolveSchema(entry.getValue(), knownRecordSchemas);
        return (keySchema == entry.getKey() && valueSchema == entry.getValue()) ?
                schema : Schemas.mapOf(keySchema, valueSchema);
      case UNION:
        List<Schema> schemaBuilder = new ArrayList<>();
        boolean changed = false;
        for (Schema input : schema.getUnionSchemas()) {
          Schema output = resolveSchema(input, knownRecordSchemas);
          if (output != input) {
            changed = true;
          }
          schemaBuilder.add(output);
        }
        return changed ? Schemas.unionOf(schemaBuilder) : schema;
      case RECORD:
        if (schema.fields == null) {
          // It is a named record that refers to previously defined record
          Schema knownSchema = knownRecordSchemas.get(schema.recordName);
          if (knownSchema != null) {
            throw new IllegalArgumentException(String.format("Undefined schema %s", schema.recordName));
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

  private <A, B> Map<B, A> inverse(Map<A, B> map) {
    Map<B, A> result = new HashMap<>(map.size());
    for (Map.Entry<A, B> entry : map.entrySet()) {
      if (result.containsKey(entry.getValue())) {
        throw new IllegalArgumentException("Cannot inverse map with duplicate values");
      }
      result.put(entry.getValue(), entry.getKey());
    }
    return result;
  }
}
