/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An abstract base class for generating schema. It knows how to generate
 * most of the supported data type, except record (bean class) type, which
 * it delegates to child class.
 */
public abstract class AbstractSchemaGenerator implements SchemaGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractSchemaGenerator.class);

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

      // Some extra ones for some common build-in types. Need corresponding handling in DatumReader/Writer
      .put(URI.class, Schema.of(Schema.Type.STRING))
      .put(URL.class, Schema.of(Schema.Type.STRING))
      .put(UUID.class, Schema.of(Schema.Type.BYTES))
      .build();

  @Override
  public final Schema generate(Type type) throws UnsupportedTypeException {
    return generate(type, true);
  }

  @Override
  public final Schema generate(Type type, boolean acceptRecursiveTypes) throws UnsupportedTypeException {
    Set<String> knownRecords = ImmutableSet.of();
    return doGenerate(TypeToken.of(type), knownRecords, acceptRecursiveTypes);
  }

  /**
   * Actual schema generation. It recursively resolves container types.
   *
   * @param typeToken    Encapsulate the Java type for generating a {@link Schema}.
   * @param knownRecords Set of record names that has the schema already generated. It is used for
   *                     recursive class field references.
   * @param acceptRecursion Whether to tolerate type recursion. If false, will throw UnsupportedTypeException if
   *                        a recursive type is encountered.
   * @return A {@link Schema} representing the given java {@link Type}.
   * @throws UnsupportedTypeException Indicates schema generation is not support for the given java {@link Type}.
   */
  @SuppressWarnings("unchecked")
  protected final Schema doGenerate(TypeToken<?> typeToken, Set<String> knownRecords, boolean acceptRecursion)
    throws UnsupportedTypeException {
    Type type = typeToken.getType();
    Class<?> rawType = typeToken.getRawType();

    if (SIMPLE_SCHEMAS.containsKey(rawType)) {
      return SIMPLE_SCHEMAS.get(rawType);
    }

    // Enum type, simply use all the enum constants for ENUM schema.
    if (rawType.isEnum()) {
      return Schema.enumWith((Class<Enum<?>>) rawType);
    }

    // Java array, use ARRAY schema.
    if (rawType.isArray()) {
      Schema componentSchema = doGenerate(TypeToken.of(rawType.getComponentType()), knownRecords, acceptRecursion);
      if (rawType.getComponentType().isPrimitive()) {
        return Schema.arrayOf(componentSchema);
      }
      return Schema.arrayOf(Schema.unionOf(componentSchema, Schema.of(Schema.Type.NULL)));
    }

    if (!(type instanceof Class || type instanceof ParameterizedType)) {
      throw new UnsupportedTypeException("Type " + type + " is not supported. " +
                                         "Only Class or ParameterizedType are supported.");
    }

    // Any parameterized Collection class would be represented by ARRAY schema.
    if (Collection.class.isAssignableFrom(rawType)) {
      if (!(type instanceof ParameterizedType)) {
        throw new UnsupportedTypeException("Only supports parameterized Collection type.");
      }
      TypeToken<?> componentType = typeToken.resolveType(((ParameterizedType) type).getActualTypeArguments()[0]);
      Schema componentSchema = doGenerate(componentType, knownRecords, acceptRecursion);
      return Schema.arrayOf(Schema.unionOf(componentSchema, Schema.of(Schema.Type.NULL)));
    }

    // Java Map, use MAP schema.
    if (Map.class.isAssignableFrom(rawType)) {
      if (!(type instanceof ParameterizedType)) {
        throw new UnsupportedTypeException("Only supports parameterized Map type.");
      }
      Type[] typeArgs = ((ParameterizedType) type).getActualTypeArguments();
      TypeToken<?> keyType = typeToken.resolveType(typeArgs[0]);
      TypeToken<?> valueType = typeToken.resolveType(typeArgs[1]);

      Schema valueSchema = doGenerate(valueType, knownRecords, acceptRecursion);

      return Schema.mapOf(doGenerate(keyType, knownRecords, acceptRecursion),
                          Schema.unionOf(valueSchema, Schema.of(Schema.Type.NULL)));
    }

    // Any Java class, class name as the record name.
    String recordName = typeToken.getRawType().getName();
    if (knownRecords.contains(recordName)) {
      // Record already seen before
      if (acceptRecursion) {
        // simply create a reference RECORD schema by the name.
        return Schema.recordOf(recordName);
      } else {
        throw new UnsupportedTypeException("Recursive type not supported for class " + recordName);
      }
    }

    // Delegate to child class to generate RECORD schema.
    return generateRecord(typeToken, knownRecords, acceptRecursion);
  }

  /**
   * Generates a RECORD schema of the given type.
   *
   * @param typeToken Type of the record.
   * @param knownRecords Set of record names that schema has already been generated.
   * @param acceptRecursiveTypes Whether to tolerate type recursion. If false, will throw UnsupportedTypeException if
   *                             a recursive type is encountered.
   * @return An instance of {@link Schema}
   * @throws UnsupportedTypeException
   */
  protected abstract Schema generateRecord(TypeToken<?> typeToken,
                                           Set<String> knownRecords,
                                           boolean acceptRecursiveTypes) throws UnsupportedTypeException;
}
