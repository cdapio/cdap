package com.continuuity.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

    public boolean isSimpleType() {
      return simpleType;
    }
  }

  public static final class Field {
    private final String name;
    private final Schema schema;

    public static Field of(String name, Schema schema) {
      return new Field(name, schema);
    }

    private Field(String name, Schema schema) {
      this.name = name;
      this.schema = schema;
    }

    public String getName() {
      return name;
    }

    public Schema getSchema() {
      return schema;
    }
  }

  public static Schema of(Type type) {
    Preconditions.checkArgument(type.isSimpleType(), "Type %s is not a simple type.", type);
    return new Schema(type, null, null, null, null, null, null, null);
  }

  public static Schema enumWith(String...values) {
    return enumWith(ImmutableList.copyOf(values));
  }

  public static Schema enumWith(Iterable<String> values) {
    Set<String> uniqueValues = ImmutableSet.copyOf(values);
    Preconditions.checkArgument(uniqueValues.size() > 0, "No enum value provided.");
    Preconditions.checkArgument(Iterables.size(values) == uniqueValues.size(), "Duplicate enum value is not allowed.");
    return new Schema(Type.ENUM, uniqueValues, null, null, null, null, null, null);
  }

  public static Schema enumWith(Class<Enum<?>> enumClass) {
    Enum<?>[] enumConstants = enumClass.getEnumConstants();
    String[] names = new String[enumConstants.length];
    for (int i = 0; i < enumConstants.length; i++) {
      names[i] = enumConstants[i].name();
    }
    return enumWith(names);
  }

  public static Schema arrayOf(Schema componentSchema) {
    return new Schema(Type.ARRAY, null, componentSchema, null, null, null, null, null);
  }

  public static Schema mapOf(Schema keySchema, Schema valueSchema) {
    return new Schema(Type.MAP, null, null, keySchema, valueSchema, null, null, null);
  }

  public static Schema recordOf(String name) {
    Preconditions.checkNotNull(name, "Record name cannot be null.");
    return new Schema(Type.RECORD, null, null, null, null, name, null, null);
  }

  public static Schema recordOf(String name, Field...fields) {
    return recordOf(name, ImmutableList.copyOf(fields));
  }

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

  public static Schema unionOf(Schema...schemas) {
    return unionOf(ImmutableList.copyOf(schemas));
  }

  public static Schema unionOf(Iterable<Schema> schemas) {
    List<Schema> schemaList = ImmutableList.copyOf(schemas);
    Preconditions.checkArgument(schemaList.size() > 0, "No union schema provided.");
    return new Schema(Type.UNION, null, null, null, null, null, null, schemaList);
  }

  private final Type type;
  private final Set<String> enumValues;
  private final Schema componentSchema;

  private final Schema keySchema;
  private final Schema valueSchema;

  private final String recordName;
  private final Map<String, Field> fieldMap;
  private final List<Field> fields;

  private final List<Schema> unionSchemas;

  private String schemaString;

  private Schema(Type type, Set<String> enumValues, Schema componentSchema, Schema keySchema, Schema valueSchema, String recordName, Map<String, Field> fieldMap, List<Schema> unionSchemas) {
    this.type = type;
    this.enumValues = enumValues;
    this.componentSchema = componentSchema;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.recordName = recordName;
    this.fieldMap = populateRecordFields(fieldMap);
    this.fields = this.fieldMap == null ? null : ImmutableList.copyOf(this.fieldMap.values());
    this.unionSchemas = unionSchemas;
  }

  public Type getType() {
    return type;
  }

  public Set<String> getEnumValues() {
    return enumValues;
  }

  public Schema getComponentSchema() {
    return componentSchema;
  }

  public Map.Entry<Schema, Schema> getMapSchema() {
    return Maps.immutableEntry(keySchema, valueSchema);
  }

  public List<Field> getFields() {
    return fields;
  }

  public Field getField(String name) {
    return fieldMap.get(name);
  }

  public List<Schema> getUnionSchemas() {
    return unionSchemas;
  }

  @Override
  public String toString() {
    // The follow logic is thread safe, as all the fields buildString() needs are immutable.
    // It's possible that buildString() get triggered multiple times, but they should yield the same result.
    String str = schemaString;
    if (str == null) {
      str = buildString();
      schemaString = str;
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

    return toString().equals(other.toString());
  }

  @Override
  public int hashCode() {
    return schemaString.hashCode();
  }

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

  private String buildString() {
    if (type.isSimpleType()) {
      return '"' + type.name().toString().toLowerCase() + '"';
    }
    StringBuilder builder = new StringBuilder();
    JsonWriter writer = new JsonWriter(CharStreams.asWriter(builder));
    try {
      Set<String> knownRecord = Sets.newHashSet();
      buildString(writer, knownRecord);
      writer.close();
      return builder.toString();
    } catch (IOException e) {
      // It should never throw IOException on the StringBuilder Writer, if it does, something very wrong.
      throw Throwables.propagate(e);
    }
  }

  private JsonWriter buildString(JsonWriter writer, Set<String> knownRecord) throws IOException {
    if (type.isSimpleType()) {
      return writer.value(type.name().toString().toLowerCase());
    }

    if (type == Type.UNION) {
      writer.beginArray();
      for (Schema schema : unionSchemas) {
        schema.buildString(writer, knownRecord);
      }
      return writer.endArray();
    }

    writer.beginObject()
      .name("type").value(type.name().toLowerCase());
    switch (type) {
      case ENUM:
        writer.name("symbols")
          .beginArray();
        for (String enumValue : enumValues) {
          writer.value(enumValue);
        }
        writer.endArray();
        break;

      case ARRAY:
        componentSchema.buildString(writer.name("items"), knownRecord);
        break;

      case MAP:
        keySchema.buildString(writer.name("keys"), knownRecord);
        valueSchema.buildString(writer.name("values"), knownRecord);
        break;

      case RECORD:
        writer.name("name").value(recordName);
        if (!knownRecord.contains(recordName)) {
          knownRecord.add(recordName);
          writer.name("fields")
            .beginArray();
          for (Field field : fields) {
            writer.beginObject()
              .name("name").value(field.getName());
            field.getSchema().buildString(writer.name("type"), knownRecord)
              .endObject();
          }
          writer.endArray();
        }
        break;
    }
    writer.endObject();

    return writer;
  }
}
