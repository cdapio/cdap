package com.continuuity.io;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An abstract base class for generating schema. It knows how to generate
 * most of the supported data type, except record (bean class) type, which
 * it delegates to child class.
 */
public abstract class AbstractSchemaGenerator implements SchemaGenerator {

  /**
   * Mapping Java types into Schemas for simple data types.
   */
  private static final Map<Class<?>, Schema> SIMPLE_SCHEMAS =
    ImmutableMap.<Class<?>, Schema>builder()
                .put(Boolean.TYPE, Schema.of(Schema.Type.BOOLEAN))
                .put(Byte.TYPE, Schema.of(Schema.Type.INT))
                .put(Character.TYPE, Schema.of(Schema.Type.INT))
                .put(Short.TYPE, Schema.of(Schema.Type.INT))
                .put(Integer.TYPE, Schema.of(Schema.Type.INT))
                .put(Long.TYPE, Schema.of(Schema.Type.LONG))
                .put(Float.TYPE, Schema.of(Schema.Type.FLOAT))
                .put(Double.TYPE, Schema.of(Schema.Type.DOUBLE))

                .put(Boolean.class, Schema.of(Schema.Type.BOOLEAN))
                .put(Byte.class, Schema.of(Schema.Type.INT))
                .put(Character.class, Schema.of(Schema.Type.INT))
                .put(Short.class, Schema.of(Schema.Type.INT))
                .put(Integer.class, Schema.of(Schema.Type.INT))
                .put(Long.class, Schema.of(Schema.Type.LONG))
                .put(Float.class, Schema.of(Schema.Type.FLOAT))
                .put(Double.class, Schema.of(Schema.Type.DOUBLE))

                .put(String.class, Schema.of(Schema.Type.STRING))
                .put(byte[].class, Schema.of(Schema.Type.BYTES))
                .put(ByteBuffer.class, Schema.of(Schema.Type.BYTES))
                .build();

  @Override
  public final Schema generate(Type type) throws UnsupportedTypeException {
    Set<String> knownRecords = Sets.newHashSet();
    return doGenerate(TypeToken.of(type), knownRecords);
  }

  /**
   * Actual schema generation. It recursively resolves container types.
   *
   * @param typeToken Encapsulate the Java type for generating a {@link Schema}.
   * @param knownRecords Set of record names that has the schema already generated. It is used for
   *                     recursive class field references.
   * @return A {@link Schema} representing the given java {@link Type}.
   * @throws UnsupportedTypeException Indicates schema generation is not support for the given java {@link Type}.
   */
  protected final Schema doGenerate(TypeToken<?> typeToken, Set<String> knownRecords) throws UnsupportedTypeException {
    Type type = typeToken.getType();
    if (!(type instanceof Class) && !(type instanceof ParameterizedType)) {
      throw new UnsupportedTypeException("Type " + type + " is not supported. " +
                                         "Only Class or ParameterizedType are supported.");
    }
    Class<?> rawType = typeToken.getRawType();

    if (SIMPLE_SCHEMAS.containsKey(rawType)) {
      return SIMPLE_SCHEMAS.get(rawType);
    }

    // Enum type, simply use all the enum constants for ENUM schema.
    if (rawType.isEnum()) {
      return Schema.enumWith((Class<Enum<?>>)rawType);
    }

    // Java array, use ARRAY schema.
    if (rawType.isArray()) {
      return Schema.arrayOf(doGenerate(TypeToken.of(rawType), knownRecords));
    }

    // Any parameterized Collection class would be represented by ARRAY schema.
    if (Collection.class.isAssignableFrom(rawType)) {
      if (!(type instanceof ParameterizedType)) {
        throw new UnsupportedTypeException("Only supports parameterized Collection type.");
      }
      TypeToken<?> componentType = typeToken.resolveType(((ParameterizedType)type).getActualTypeArguments()[0]);
      return Schema.arrayOf(doGenerate(componentType, knownRecords));
    }

    // Java Map, use MAP schema.
    if (Map.class.isAssignableFrom(rawType)) {
      if (!(type instanceof ParameterizedType)) {
        throw new UnsupportedTypeException("Only supports parameterized Map type.");
      }
      Type[] typeArgs = ((ParameterizedType) type).getActualTypeArguments();
      TypeToken<?> keyType = typeToken.resolveType(typeArgs[0]);
      TypeToken<?> valueType = typeToken.resolveType(typeArgs[1]);

      return Schema.mapOf(doGenerate(keyType, knownRecords), doGenerate(valueType, knownRecords));
    }

    // Any Java class, class name as the record name.
    String recordName = typeToken.getRawType().getName();
    if (knownRecords.contains(recordName)) {
      // Record already seen before, simply create a reference RECORD schema by the name.
      return Schema.recordOf(recordName);
    }

    // Delegate to child class to generate RECORD schema.
    return generateRecord(typeToken, knownRecords);
  }

  /**
   * Generates a RECORD schema of the given type.
   * @param typeToken
   * @param knownRecords
   * @return
   * @throws UnsupportedTypeException
   */
  protected abstract Schema generateRecord(TypeToken<?> typeToken, Set<String> knownRecords) throws UnsupportedTypeException;
}
