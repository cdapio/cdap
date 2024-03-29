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

package io.cdap.cdap.internal.io;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An abstract base class for generating schema. It knows how to generate most of the supported data
 * type, except record (bean class) type, which it delegates to child class.
 */
public abstract class AbstractSchemaGenerator implements SchemaGenerator {

  /**
   * Mapping Java types into Schemas for simple data types.
   */
  private static final Map<Class<?>, Schema> SIMPLE_SCHEMAS;

  static {
    Map<Class<?>, Schema> simpleSchemas = new HashMap<>();

    simpleSchemas.put(Boolean.TYPE, Schema.of(Schema.Type.BOOLEAN));
    simpleSchemas.put(Byte.TYPE, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Character.TYPE, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Short.TYPE, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Integer.TYPE, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Long.TYPE, Schema.of(Schema.Type.LONG));
    simpleSchemas.put(Float.TYPE, Schema.of(Schema.Type.FLOAT));
    simpleSchemas.put(Double.TYPE, Schema.of(Schema.Type.DOUBLE));

    simpleSchemas.put(Boolean.class, Schema.of(Schema.Type.BOOLEAN));
    simpleSchemas.put(Byte.class, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Character.class, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Short.class, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Integer.class, Schema.of(Schema.Type.INT));
    simpleSchemas.put(Long.class, Schema.of(Schema.Type.LONG));
    simpleSchemas.put(Float.class, Schema.of(Schema.Type.FLOAT));
    simpleSchemas.put(Double.class, Schema.of(Schema.Type.DOUBLE));

    simpleSchemas.put(String.class, Schema.of(Schema.Type.STRING));
    simpleSchemas.put(byte[].class, Schema.of(Schema.Type.BYTES));
    simpleSchemas.put(ByteBuffer.class, Schema.of(Schema.Type.BYTES));

    // TODO: (CDAP-16919) Convert Object class mapping to union of all simple schema types.
    simpleSchemas.put(Object.class, Schema.of(Schema.Type.NULL));

    // Some extra ones for some common build-in types. Need corresponding handling in DatumReader/Writer
    simpleSchemas.put(URI.class, Schema.of(Schema.Type.STRING));
    simpleSchemas.put(URL.class, Schema.of(Schema.Type.STRING));
    simpleSchemas.put(UUID.class, Schema.of(Schema.Type.BYTES));

    SIMPLE_SCHEMAS = Collections.unmodifiableMap(simpleSchemas);
  }

  @Override
  public final Schema generate(Type type) throws UnsupportedTypeException {
    return generate(type, true);
  }

  @Override
  public final Schema generate(Type type, boolean acceptRecursiveTypes)
      throws UnsupportedTypeException {
    Set<String> knownRecords = Collections.emptySet();
    return doGenerate(TypeToken.of(type), knownRecords, acceptRecursiveTypes);
  }

  /**
   * Actual schema generation. It recursively resolves container types.
   *
   * @param typeToken Encapsulate the Java type for generating a {@link Schema}.
   * @param knownRecords Set of record names that has the schema already generated. It is used
   *     for recursive class field references.
   * @param acceptRecursion Whether to tolerate type recursion. If false, will throw
   *     UnsupportedTypeException if a recursive type is encountered.
   * @return A {@link Schema} representing the given java {@link Type}.
   * @throws UnsupportedTypeException Indicates schema generation is not support for the given
   *     java {@link Type}.
   */
  @SuppressWarnings("unchecked")
  protected final Schema doGenerate(TypeToken<?> typeToken, Set<String> knownRecords,
      boolean acceptRecursion)
      throws UnsupportedTypeException {
    Type type = typeToken.getType();
    Class<?> rawType = typeToken.getRawType();

    // Object type was introduced in SIMPLE_SCHEMAS to satisfy Java Object usage in ETLConfig.
    // Do not consider Java Object as simple schema for schema generation purpose.
    if (SIMPLE_SCHEMAS.containsKey(rawType)) {
      return SIMPLE_SCHEMAS.get(rawType);
    }

    // Enum type, simply use all the enum constants for ENUM schema.
    if (rawType.isEnum()) {
      return Schema.enumWith((Class<Enum<?>>) rawType);
    }

    // Java array, use ARRAY schema.
    if (rawType.isArray()) {
      Schema componentSchema = doGenerate(TypeToken.of(rawType.getComponentType()), knownRecords,
          acceptRecursion);
      if (rawType.getComponentType().isPrimitive()) {
        return Schema.arrayOf(componentSchema);
      }
      return Schema.arrayOf(Schema.unionOf(componentSchema, Schema.of(Schema.Type.NULL)));
    }

    if (!(type instanceof Class || type instanceof ParameterizedType)) {
      throw new UnsupportedTypeException("Type " + type + " is not supported. "
          + "Only Class or ParameterizedType are supported.");
    }

    // Any parameterized Collection class would be represented by ARRAY schema.
    if (Collection.class.isAssignableFrom(rawType)) {
      if (!(type instanceof ParameterizedType)) {
        throw new UnsupportedTypeException("Only supports parameterized Collection type.");
      }
      TypeToken<?> componentType = typeToken.resolveType(
          ((ParameterizedType) type).getActualTypeArguments()[0]);
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
   * @param acceptRecursiveTypes Whether to tolerate type recursion. If false, will throw
   *     UnsupportedTypeException if a recursive type is encountered.
   * @return An instance of {@link Schema}
   */
  protected abstract Schema generateRecord(TypeToken<?> typeToken,
      Set<String> knownRecords,
      boolean acceptRecursiveTypes) throws UnsupportedTypeException;
}
