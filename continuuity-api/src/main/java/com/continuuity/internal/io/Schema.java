/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.CharStreams;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents schema of data types.
 */
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
  }

  /**
   * Creates a {@link Schema} for the given type. The type given must be a
   * {@link Schema.Type#isSimpleType() Simple Type}.
   *
   * @param type Type of the schema to create.
   * @return A {@link Schema} with the given type.
   */
  public static Schema of(Type type) {
    Preconditions.checkArgument(type.isSimpleType(), "Type %s is not a simple type.", type);
    return new Schema(type, null, null, null, null, null, null, null);
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
    return enumWith(ImmutableList.copyOf(values));
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
    Set<String> uniqueValues = ImmutableSet.copyOf(values);
    Preconditions.checkArgument(uniqueValues.size() > 0, "No enum value provided.");
    Preconditions.checkArgument(Iterables.size(values) == uniqueValues.size(), "Duplicate enum value is not allowed.");
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
    Preconditions.checkNotNull(name, "Record name cannot be null.");
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
    return recordOf(name, ImmutableList.copyOf(fields));
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
    Preconditions.checkNotNull(name, "Record name cannot be null.");
    ImmutableMap.Builder<String, Field> fieldMapBuilder = ImmutableMap.builder();
    for (Field field : fields) {
      fieldMapBuilder.put(field.getName(), field);
    }
    Map<String, Field> fieldMap = fieldMapBuilder.build();
    Preconditions.checkArgument(fieldMap.size() > 0, "No record field provided for %s", name);
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
    return unionOf(ImmutableList.copyOf(schemas));
  }

  /**
   * Creates a {@link Type#UNION UNION} {@link Schema} which represents a union of all the given schemas.
   * The ordering of the schemas inside the union would be the same as the {@link Iterable#iterator()} order.
   *
   * @param schemas All the {@link Schema Schemas} constitutes the union.
   * @return A {@link Schema} of {@link Type#UNION UNION} type.
   */
  public static Schema unionOf(Iterable<Schema> schemas) {
    List<Schema> schemaList = ImmutableList.copyOf(schemas);
    Preconditions.checkArgument(schemaList.size() > 0, "No union schema provided.");
    return new Schema(Type.UNION, null, null, null, null, null, null, schemaList);
  }

  private final Type type;

  private final BiMap<String, Integer> enumValues;
  private final BiMap<Integer, String> enumIndexes;

  private final Schema componentSchema;

  private final Schema keySchema;
  private final Schema valueSchema;
  private final Map.Entry<Schema, Schema> mapSchema;

  private final String recordName;
  private final Map<String, Field> fieldMap;
  private final List<Field> fields;

  private final List<Schema> unionSchemas;

  private String schemaString;
  private SchemaHash schemaHash;

  private Schema(Type type, Set<String> enumValues, Schema componentSchema, Schema keySchema, Schema valueSchema,
                 String recordName, Map<String, Field> fieldMap, List<Schema> unionSchemas) {
    this.type = type;
    this.enumValues = createIndex(enumValues);
    this.enumIndexes = this.enumValues == null ? null : this.enumValues.inverse();
    this.componentSchema = componentSchema;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.mapSchema = (keySchema == null || valueSchema == null) ? null : Maps.immutableEntry(keySchema, valueSchema);
    this.recordName = recordName;
    this.fieldMap = populateRecordFields(fieldMap);
    this.fields = this.fieldMap == null ? null : ImmutableList.copyOf(this.fieldMap.values());
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
    Multimap<String, String> recordCompared = HashMultimap.create();
    return checkCompatible(target, recordCompared);
  }

  private boolean checkCompatible(Schema target, Multimap<String, String> recordCompared) {
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
          if (!recordCompared.containsEntry(recordName, target.recordName)) {
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
  private <V> BiMap<V, Integer> createIndex(Set<V> values) {
    if (values == null) {
      return null;
    }

    ImmutableBiMap.Builder<V, Integer> builder = ImmutableBiMap.builder();
    int idx = 0;
    for (V value : values) {
      builder.put(value, idx++);
    }
    return builder.build();
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

    Map<String, Schema> knownRecordSchemas = Maps.newHashMap();
    knownRecordSchemas.put(recordName, this);
    ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();

    for (Map.Entry<String, Field> fieldEntry : fields.entrySet()) {
      String fieldName = fieldEntry.getKey();
      Field field = fieldEntry.getValue();
      Schema fieldSchema = resolveSchema(field.getSchema(), knownRecordSchemas);

      if (fieldSchema == field.getSchema()) {
        builder.put(fieldName, field);
      } else {
        builder.put(fieldName, Field.of(fieldName, fieldSchema));
      }
    }

    return builder.build();
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
        ImmutableList.Builder<Schema> schemaBuilder = ImmutableList.builder();
        boolean changed = false;
        for (Schema input : schema.getUnionSchemas()) {
          Schema output = resolveSchema(input, knownRecordSchemas);
          if (output != input) {
            changed = true;
          }
          schemaBuilder.add(output);
        }
        return changed ? Schema.unionOf(schemaBuilder.build()) : schema;
      case RECORD:
        if (schema.fields == null) {
          // It is a named record that refers to previously defined record
          Schema knownSchema = knownRecordSchemas.get(schema.recordName);
          Preconditions.checkArgument(knownSchema != null, "Undefined schema %s", schema.recordName);
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
    StringBuilder builder = new StringBuilder();
    JsonWriter writer = new JsonWriter(CharStreams.asWriter(builder));
    try {
      new com.continuuity.internal.io.SchemaTypeAdapter().write(writer, this);
      writer.close();
      return builder.toString();
    } catch (IOException e) {
      // It should never throw IOException on the StringBuilder Writer, if it does, something very wrong.
      throw Throwables.propagate(e);
    }
  }
}
