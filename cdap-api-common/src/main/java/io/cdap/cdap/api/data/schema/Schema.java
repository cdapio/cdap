/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.api.data.schema;

import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.internal.io.SQLSchemaParser;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;

import java.io.IOException;
import java.io.ObjectStreamException;
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
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * This class represents schema of data types.
 */
@Beta
public final class Schema implements Serializable {
  private static final SchemaTypeAdapter SCHEMA_TYPE_ADAPTER = new SchemaTypeAdapter();
  private static final long serialVersionUID = -1891891892562027345L;
  private int precision;
  private int scale;
  private int fixedSize;

  private String fixedName;

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
   * Logical type to represent date and time using {@link Schema}. Schema of logical types is the schema of primitive
   * types with an extra attribute `logicalType`.
   *
   * Logical type DATE relies on INT as underlying primitive type
   * Logical type TIMESTAMP_MILLIS relies on LONG as underlying primitive type
   * Logical type TIMESTAMP_MICROS relies on LONG as underlying primitive type
   * Logical type TIME_MILLIS relies on INT as underlying primitive type
   * Logical type TIME_MICROS relies on LONG as underlying primitive type
   * Logical type DECIMAL relies on BYTES as underlying primitive type
   * Logical type DATETIME relies on STRING as underlying primitive type
   * Logical type FIXED relies on BYTES as underlying primitive type
   *
   * For example, the json schema for logicaltype date will be as below:
   * {
   *   "type" : "int",
   *   "logicalType" : "date"
   * }
   *
   * Example schema for logicaltype decimal will be as below:
   * {
   *   "type" : "bytes",
   *   "logicalType" : "decimal",
   *   "precision" : precision,
   *   "scale" : scale
   * }
   * where precision is an int representing maximum number of unscaled digits in a decimal and
   * positive scale is an int representing number of digits to the right of decimal number whereas
   * negative scale is an int representing number of zeros appended to decimal number.
   *
   * For example,
   * An unscaled value of 102 and scale 1 represents the number 10.2 with precision 3.
   * An unscaled value of 111 and scale -1 represents the number 1110 with precision 3.
   *
   * Example json schema for logicaltype datetime will be as below.
   * {
   *   "type": "string",
   *   "logicalType": "datetime"
   * }
   * Note that the string value is expected to be in ISO 8601 format without offset(timezone).
   */
  public enum LogicalType {
    DATE(Type.INT, "date"),
    TIMESTAMP_MILLIS(Type.LONG, "timestamp-millis"),
    TIMESTAMP_MICROS(Type.LONG, "timestamp-micros"),
    TIME_MILLIS(Type.INT, "time-millis"),
    TIME_MICROS(Type.LONG, "time-micros"),
    DECIMAL(Type.BYTES, "decimal"),
    DATETIME(Type.STRING, "datetime"),
    FIXED(Type.BYTES, "fixed");

    private final Type type;
    private final String token;

    private static final Map<String, LogicalType> LOOKUP_BY_TOKEN;
    static {
      Map<String, LogicalType> map = new HashMap<>();
      for (LogicalType logicalType : values()) {
        map.put(logicalType.token, logicalType);
      }
      LOOKUP_BY_TOKEN = Collections.unmodifiableMap(map);
    }

    /**
     * Creates {@link LogicalType} with underlying primitive type.
     *
     * @param type primitive type on which this {@link LogicalType} relies on
     * @param token token string associated with {@link LogicalType}
     */
    LogicalType(Type type, String token) {
      this.type = type;
      this.token = token;
    }

    /**
     * @return returns the token string associated with logical type.
     */
    public String getToken() {
      return token;
    }

    /**
     * Returns {@link LogicalType} associated with the token string provided.
     *
     * @param token token string for the logical type
     * @return logical type associated with the token string
     */
    public static LogicalType fromToken(String token) {
      LogicalType logicalType = LOOKUP_BY_TOKEN.get(token);
      if (logicalType != null) {
        return logicalType;
      }
      throw new IllegalArgumentException("Unknown logical type for token: " + token);
    }
  }

  /**
   * Represents a field inside a {@link Type#RECORD} schema.
   */
  public static final class Field implements Serializable {
    private static final long serialVersionUID = 5423721270457378454L;
    private final String name;
    private Schema schema;

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

    private void setSchema(Schema schema) {
      this.schema = schema;
    }

    @Override
    public String toString() {
      return String.format("{name: %s, schema: %s}", name, schema);
    }
  }

  /**
   * Simple serialized form of schema that allow to use {@link SchemaCache}
   */
  private static class SerializedForm implements Serializable {
    private final String schemaCacheStr;
    private final String json;

    private SerializedForm(String schemaCacheStr, String json) {
      this.schemaCacheStr = schemaCacheStr;
      this.json = json;
    }

    Object readResolve() throws ObjectStreamException {
      return SchemaCache.fromJson(schemaCacheStr, json);
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
   * Java serialization method that allows to replace object written with a substitute.
   * Later {@link SerializedForm#readResolve()} will do a schema resolution on deserialization.
   * @see Serializable for details on writeReplace/readResolve contract.
   */
  private Object writeReplace() throws ObjectStreamException {
    return new SerializedForm(getSchemaHash().toString(), toString());
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
    return new Schema(type, null, null, null, null, null, null, null, null, null, 0, 0, 0);
  }

  /**
   * Creates a {@link Schema} for the given logical type. The logical type given must be a
   * {@link Schema.LogicalType}.
   * @param logicalType LogicalType of the schema to create.
   * @return A {@link Schema} with the given logical type.
   */
  public static Schema of(LogicalType logicalType) {
    return new Schema(logicalType.type, logicalType, null, null, null, null, null, null, null, null, 0, 0, 0);
  }

  /**
   * Creates a {@link Schema} for Decimal logical type with provided precision.
   * @param precision positive precision for decimal number
   * @return A {@link Schema} with the decimal logical type
   */
  public static Schema decimalOf(int precision) {
    return decimalOf(precision, 0);
  }

  /**
   * Creates a {@link Schema} for Decimal logical type with provided precision and scale.
   * @param precision positive precision for decimal number
   * @param scale scale for the decimal number
   * @return A {@link Schema} with the decimal logical type
   */
  public static Schema decimalOf(int precision, int scale) {
    return new Schema(Type.BYTES, LogicalType.DECIMAL, null, null, null,
                      null, null, null, null, null, precision, scale, 0);
  }

  /**
   * Creates a {@link Schema} for Fixed logical type with provided precision and scale.
   * @param fixedSize is the size of fixed type
   * @param fixedName is the name of fixed type
   * @return A {@link Schema} with the fixed logical type
   */
  public static Schema fixedOf(int fixedSize, String fixedName) {
    return new Schema(Type.BYTES, LogicalType.FIXED, null, null, null, null,
       null, fixedName, null, null, 0, 0, fixedSize);
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
    return enumWith(null, values);
  }

  /**
   * Creates a {@link Schema} of {@link Type#ENUM ENUM} type, with the given name and values.
   * The set of values given should be unique and must contains at least one value.
   * The ordering of values in the enum type schema would be the same as the {@link Iterable#iterator()} order.
   *
   * @param enumName Optional enum name.
   * @param values Enum values.
   * @return A {@link Schema} of {@link Type#ENUM ENUM} type.
   */
  public static Schema enumWith(@Nullable String enumName, Iterable<String> values) {
    Set<String> uniqueValues = new LinkedHashSet<>();
    for (String value : values) {
      if (!uniqueValues.add(value)) {
        throw new IllegalArgumentException("Duplicate enum value is not allowed.");
      }
    }
    if (enumName == null && uniqueValues.isEmpty()) {
      throw new IllegalArgumentException("No enum value provided.");
    }
    return new Schema(Type.ENUM, null, enumName, uniqueValues, null, null, null, null, null, null, 0, 0, 0);
  }

  /**
   * Creates a {@link Schema} of {@link Type#ENUM ENUM} type, with values extracted from the given {@link Enum} class.
   * The ordering of values in the enum type schema would be the same as the {@link Enum#ordinal()} order.
   *
   * @param enumClass Enum values.
   * @return A {@link Schema} of {@link Type#ENUM ENUM} type.
   */
  public static Schema enumWith(Class<? extends Enum<?>> enumClass) {
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
    return new Schema(Type.ARRAY, null, null, null, componentSchema, null, null, null, null, null, 0, 0, 0);
  }

  /**
   * Creates a {@link Type#MAP MAP} {@link Schema} of the given key and value types.
   * @param keySchema Schema of the map key.
   * @param valueSchema Schema of the map value
   * @return A {@link Schema} of {@link Type#MAP MAP} type.
   */
  public static Schema mapOf(Schema keySchema, Schema valueSchema) {
    return new Schema(Type.MAP, null, null, null, null, keySchema, valueSchema, null, null, null, 0, 0, 0);
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
    return new Schema(Type.RECORD, null, null, null, null, null, null, name, null, null, 0, 0, 0);
  }

  /**
   * Creates a {@link Type#RECORD RECORD} {@link Schema} with a name based its {@link Field Fields}.
   * The ordering of the fields inside the record would be the same as the one being passed in.
   *
   * @param fields All the fields that the record contains.
   * @return A {@link Schema} of {@link Type#RECORD RECORD} type.
   */
  public static Schema recordOf(Field...fields) {
    return recordOf(Arrays.asList(fields));
  }

  /**
   * Creates a {@link Type#RECORD RECORD} {@link Schema} with a name based on its {@link Field Fields}.
   * The ordering of the fields inside the record would be the same as the {@link Iterable#iterator()} order.
   *
   * @param fields All the fields that the record contains.
   * @return A {@link Schema} of {@link Type#RECORD RECORD} type.
   */
  public static Schema recordOf(Iterable<Field> fields) {
    // We generate a temporary random name for the record.
    // The name has to be different from all its child schemas, as AVRO does not allow to redefine a record
    // The name will not be part of the final hash
    String tempName = UUID.randomUUID().toString().replace("-", "");
    Schema tempRecord = recordOf(tempName, fields);

    // SchemaHash ignores the record name when receiving false as its second parameter.
    SchemaHash schemaHash = new SchemaHash(tempRecord, false);
    String hashName = schemaHash.toString();
    return recordOf(hashName, fields);
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
    return new Schema(Type.RECORD, null, null, null, null, null, null, name, fieldMap, null, 0, 0, 0);
  }

  /**
   * Creates a {@link Type#UNION UNION} {@link Schema} which represents a union of all the given schemas.
   * The ordering of the schemas inside the union would be the same as the one being passed in.
   *
   * Unions may not contain more than one schema with the same type, except for records and enums.
   * For example, unions containing two array types or two map types are not permitted.
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
   * Unions may not contain more than one schema with the same type, except for records and enums.
   * For example, unions containing two array types or two map types are not permitted.
   *
   * @param schemas All the {@link Schema Schemas} constitutes the union.
   * @return A {@link Schema} of {@link Type#UNION UNION} type.
   */
  public static Schema unionOf(Iterable<Schema> schemas) {
    List<Schema> mapSchemas = new ArrayList<>();
    List<Schema> arraySchemas = new ArrayList<>();
    List<Schema> schemaList = new ArrayList<>();
    for (Schema schema : schemas) {
      schemaList.add(schema);
      Schema.Type type = schema.getType();
      if (type == Type.MAP) {
        mapSchemas.add(schema);
      }
      if (type == Type.ARRAY) {
        arraySchemas.add(schema);
      }
    }
    if (schemaList.isEmpty()) {
      throw new IllegalArgumentException("No union schema provided.");
    }

    if (mapSchemas.size() > 1) {
      throw new IllegalArgumentException(String.format(
        "A union is not allowed to contain more than one map, but it contains %d schemas: %s",
        mapSchemas.size(), mapSchemas));
    }
    if (arraySchemas.size() > 1) {
      throw new IllegalArgumentException(String.format(
        "A union is not allowed to contain more than one array, but it contains %d schemas: %s",
        arraySchemas.size(), arraySchemas));
    }
    return new Schema(Type.UNION, null, null, null, null, null, null, null, null, schemaList, 0, 0, 0);
  }

  private final Type type;
  private final LogicalType logicalType;

  private final Map<String, Integer> enumValues;
  private final Map<Integer, String> enumIndexes;

  private final Schema componentSchema;

  private final Schema keySchema;
  private final Schema valueSchema;
  private final Map.Entry<Schema, Schema> mapSchema;

  private final String enumName;
  private final String recordName;
  private final Map<String, Field> fieldMap;
  private final List<Field> fields;

  private final List<Schema> unionSchemas;

  // No need to serialize the schemaString to save space
  // It can be recomputed on demand (and usually it is not used in the context that serialization is used)
  private transient String schemaString;
  private SchemaHash schemaHash;

  // This is a on demand cache for case insensitive field lookup. No need to serialize.
  private transient Map<String, Field> ignoreCaseFieldMap;

  private Schema(Type type,
                 @Nullable LogicalType logicalType,                                   // Not null for logical type
                 @Nullable String enumName,                                           // Optional for enum types
                 @Nullable Set<String> enumValues,                                    // Not null for enum type
                 @Nullable Schema componentSchema,                                    // Not null for array type
                 @Nullable Schema keySchema, @Nullable Schema valueSchema,            // Not null for map type
                 @Nullable String recordName, @Nullable Map<String, Field> fieldMap,  // Not null for record type
                 @Nullable List<Schema> unionSchemas,                                 // Not null for union type
                 int precision, int scale, int fixedSize) {
    if (logicalType == LogicalType.DECIMAL && precision <= 0) {
      throw new IllegalArgumentException("Schema for logical type decimal must be created using decimalOf() method.");
    }
    this.fixedSize = fixedSize;
    this.type = type;
    this.logicalType = logicalType;
    Map.Entry<Map<String, Integer>, Map<Integer, String>> enumValuesIndexes = createIndex(enumValues);
    this.enumName = enumName;
    this.enumValues = enumValuesIndexes.getKey();
    this.enumIndexes = enumValuesIndexes.getValue();
    this.componentSchema = componentSchema;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.mapSchema = (keySchema == null || valueSchema == null) ? null : new ImmutableEntry<>(keySchema, valueSchema);
    this.recordName = recordName;
    this.fieldMap = fieldMap == null ? null : copyFields(fieldMap);
    this.fields = this.fieldMap == null ? null : Collections.unmodifiableList(new ArrayList<>(this.fieldMap.values()));
    this.unionSchemas = unionSchemas == null ? null : new ArrayList<>(unionSchemas);
    this.precision = precision;
    this.scale = scale;

    // Resolve name only records. Only need this step for RECORD, UNION, or enum type schemas
    // For array and map schema types, they should already get resolved when the component type is being constructed.
    // If a FIXED type is ever added, Avro allows them to be named as well
    if (type == Type.RECORD || type == Type.UNION || type == Type.ENUM) {
      resolveSchema(this, new HashMap<>());
    }
  }

  /**
   * @return The {@link Type} that this schema represents.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return the {@link LogicalType} that this schema represents.
   */
  @Nullable
  public LogicalType getLogicalType() {
    return logicalType;
  }

  /**
   * @return The name of the enum field if it is defined. Also returns null if the field is not an enum.
   */
  @Nullable
  public String getEnumName() {
    return enumName;
  }

  /**
   * @return An immutable {@link Set} of enum values or {@code null} if this is not a {@link Type#ENUM ENUM} schema.
   *         The {@link Set#iterator()} order would be the enum values orders.
   */
  @Nullable
  public Set<String> getEnumValues() {
    if (enumValues == null) {
      return null;
    }
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
  @Nullable
  public String getEnumValue(int idx) {
    if (enumIndexes == null) {
      return null;
    }
    return enumIndexes.get(idx);
  }

  /**
   * @return The schema of the array component or {@code null} if this is not a {@link Type#ARRAY ARRAY} schema.
   */
  @Nullable
  public Schema getComponentSchema() {
    return componentSchema;
  }

  /**
   * @return An immutable {@code Map.Entry} if this is a {@code Type#MAP MAP} schema or {@code null} otherwise.
   *         The {@code Map.Entry#getKey()} would returns the key schema, while {@code Map.Entry#getValue()}
   *         would returns the value schema.
   */
  @Nullable
  public Map.Entry<Schema, Schema> getMapSchema() {
    return mapSchema;
  }

  /**
   * @return Name of the record if this is a {@link Type#RECORD RECORD} schema or {@code null} otherwise.
   */
  @Nullable
  public String getRecordName() {
    return recordName;
  }

  /**
   * @return An immutable {@link List} of record {@link Field Fields} if this is a {@link Type#RECORD RECORD} schema
   *         or {@code null} otherwise.
   */
  @Nullable
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
  @Nullable
  public Field getField(String name) {
    return getField(name, false);
  }

  /**
   * Returns the record {@link Field} of the given name.
   *
   * @param name Name of the field
   * @param ignoreCase if {@code true}, the field name is matched without case.
   * @return A {@link Field} or {@code null} if there is no such field in this record
   *         or this is not a {@link Type#RECORD RECORD} schema.
   */
  @Nullable
  public Field getField(String name, boolean ignoreCase) {
    if (fieldMap == null) {
      return null;
    }
    Field field = fieldMap.get(name);
    if (!ignoreCase || field != null) {
      return field;
    }

    // If allow ignoring case, build the ignore case map on demand.
    if (ignoreCaseFieldMap == null) {
      // Initialize the ignore case field map
      Map<String, Field> map = new HashMap<>();
      for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
        map.put(entry.getKey().toLowerCase(), entry.getValue());
      }
      ignoreCaseFieldMap = map;
    }
    return ignoreCaseFieldMap.get(name.toLowerCase());
  }

  /**
   * @return An immutable {@link List} of schemas inside this union
   *         or {@code null} if this is not a {@link Type#UNION UNION} schema.
   */
  @Nullable
  public List<Schema> getUnionSchemas() {
    return Collections.unmodifiableList(unionSchemas);
  }

  /**
   * @param idx Index to the union schemas
   * @return A {@link Schema} of the given union index or {@code null} if this is not a {@link Type#UNION UNION}
   *         schema or the given index is invalid.
   */
  @Nullable
  public Schema getUnionSchema(int idx) {
    return (unionSchemas == null || idx < 0 || unionSchemas.size() <= idx) ? null : unionSchemas.get(idx);
  }

  /**
   * Returns precision of decimal schema.
   */
  public int getPrecision() {
    return precision;
  }

  public int getFixedSize() {
    return fixedSize;
  }

  public String getFixedName() {
    return fixedName;
  }

  /**
   * Returns scale of decimal schema.
   */
  public int getScale() {
    return scale;
  }

  /**
   * Returns display name of this schema. If the schema is of logical type following mapping will be applied. If the
   * schema is not of logical type, then the method will return type name of the schema in lower case.
   *
   * {@link LogicalType#DATE} -> "date"
   * {@link LogicalType#TIME_MILLIS} -> "time of day in milliseconds"
   * {@link LogicalType#TIME_MICROS} -> "time of day in microseconds"
   * {@link LogicalType#TIMESTAMP_MILLIS} -> "timestamp in milliseconds"
   * {@link LogicalType#TIMESTAMP_MICROS} -> "timestamp in microseconds"
   * {@link LogicalType#DECIMAL} -> "decimal with precision p and scale s"
   * {@link LogicalType#DATETIME} -> "datetime in ISO-8601 format without timezone"
   * {@link LogicalType#FIXED} -> "byteBuffer of fixed size"
   *
   * @return display name of this schema.
   */
  public String getDisplayName() {
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return "date";
        case TIME_MILLIS:
          return "time of day in milliseconds";
        case TIME_MICROS:
          return "time of day in microseconds";
        case TIMESTAMP_MILLIS:
          return "timestamp in milliseconds";
        case TIMESTAMP_MICROS:
          return "timestamp in microseconds";
        case DECIMAL:
          return String.format("decimal with precision %d and scale %d", precision, scale);
        case DATETIME:
          return String.format("datetime in ISO-8601 format without timezone");
        case FIXED:
          return String.format("Fixed type of name %s and size %d", fixedName, fixedSize);
      }
    }

    return type.name().toLowerCase();
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
   * Helper method to encode this schema into json string.
   *
   * @return A json string representing this schema.
   */
  private String buildString() {
    // Return type only if this type does not represent logical type, otherwise use jsonwriter to convert logical type
    // to correct schema.
    if (type.isSimpleType() && logicalType == null) {
      return '"' + type.name().toLowerCase() + '"';
    }

    StringWriter writer = new StringWriter();
    try (JsonWriter jsonWriter = new JsonWriter(writer)) {
      SCHEMA_TYPE_ADAPTER.write(jsonWriter, this);
    } catch (IOException e) {
      // It should never throw IOException on the StringWriter, if it does, something is very wrong.
      throw new RuntimeException(e);
    }
    return writer.toString();
  }

  /**
   * Copies the given set of fields.
   */
  private static Map<String, Field> copyFields(Map<String, Field> fields) {
    Map<String, Field> result = new LinkedHashMap<>();
    for (Map.Entry<String, Field> field : fields.entrySet()) {
      result.put(field.getKey(), Field.of(field.getKey(), field.getValue().getSchema()));
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * This method is to recursively resolves all name only record schema in the given schema.
   * This method should only be constructor as this will mutate the Schema while resolving for name only records.
   *
   * @param schema The schema needs to be resolved.
   * @param knownSchemas The mapping of the already resolved schemas.
   * @return A {@link Schema} that is structurally the same as the input schema, but with all
   *         name only record schemas resolved to full schemas (i.e. with fields sets).
   *         If nothing in the given schema needs to be resolved, the same schema instance would be returned,
   *         otherwise, a new instance would be returned.
   */
  private static Schema resolveSchema(final Schema schema, final Map<String, Schema> knownSchemas) {
    switch (schema.getType()) {
      case ARRAY:
        Schema componentSchema = resolveSchema(schema.getComponentSchema(), knownSchemas);
        return (componentSchema == schema.getComponentSchema()) ? schema : Schema.arrayOf(componentSchema);
      case MAP:
        Map.Entry<Schema, Schema> entry = schema.getMapSchema();
        Schema keySchema = resolveSchema(entry.getKey(), knownSchemas);
        Schema valueSchema = resolveSchema(entry.getValue(), knownSchemas);
        return (keySchema == entry.getKey() && valueSchema == entry.getValue()) ?
          schema : Schema.mapOf(keySchema, valueSchema);
      case UNION:
        ListIterator<Schema> iterator = schema.unionSchemas.listIterator();
        while (iterator.hasNext()) {
          iterator.set(resolveSchema(iterator.next(), knownSchemas));
        }
        return schema;
      case RECORD:
        Schema knownSchema = knownSchemas.get(schema.getRecordName());
        if (knownSchema != null) {
          return knownSchema;
        }
        if (schema.fields != null) {
          knownSchemas.put(schema.getRecordName(), schema);
          for (Field field : schema.fields) {
            field.setSchema(resolveSchema(field.getSchema(), knownSchemas));
          }
        }
        return schema;
      case ENUM:
        knownSchema = knownSchemas.get(schema.getEnumName());
        if (knownSchema != null) {
          return knownSchema;
        }
        if (schema.enumValues != null) {
          knownSchemas.put(schema.getEnumName(), schema);
        }
    }
    return schema;
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
