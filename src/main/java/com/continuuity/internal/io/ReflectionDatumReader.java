package com.continuuity.internal.io;

import com.continuuity.api.io.Decoder;
import com.continuuity.api.io.Schema;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ReflectionDatumReader<T> {

  private final Schema schema;
  private final TypeToken<T> type;
  private final Map<Class<?>, InstanceCreator<?>> creators;
  private final InstanceCreatorFactory creatorFactory;

  public ReflectionDatumReader(Schema schema, TypeToken<T> type) {
    this.schema = schema;
    this.type = type;

    this.creatorFactory = new InstanceCreatorFactory();
    this.creators = Maps.newIdentityHashMap();
  }

  public T read(Decoder decoder, Schema sourceSchema) throws IOException {
    return (T)read(decoder, sourceSchema, schema, type);
  }

  private Object read(Decoder decoder, Schema sourceSchema,
                      Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {
    Schema.Type sourceType = sourceSchema.getType();
    Schema.Type targetType = targetSchema.getType();

    switch (sourceType) {
      case NULL:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return decoder.readNull();
      case BYTES:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readBytes(decoder, targetTypeToken);
      case ENUM:
        check(targetSchema.getEnumValues().containsAll(sourceSchema.getEnumValues()),
              "Source enum values missing in target. Source: %s, target: %s", sourceSchema, targetSchema);
        return sourceSchema.getEnumValue(decoder.readInt());
      case ARRAY:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readArray(decoder, sourceSchema, targetSchema, targetTypeToken);
      case MAP:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readMap(decoder, sourceSchema, targetSchema, targetTypeToken);
      case RECORD:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readRecord(decoder, sourceSchema, targetSchema, targetTypeToken);
      case UNION:
        return readUnion(decoder, sourceSchema, targetSchema, targetTypeToken);
    }
    // For simple type other than NULL and BYTES
    if (sourceType.isSimpleType()) {
      return resolveType(decoder, sourceType, targetType);
    }
    throw new IOException(String.format("Fails to resolve %s to %s", sourceSchema, targetSchema));
  }

  private Object readBytes(Decoder decoder, TypeToken<?> targetTypeToken) throws IOException {
    ByteBuffer buffer = decoder.readBytes();

    if (targetTypeToken.getRawType().equals(byte[].class)) {
      if (buffer.hasArray()) {
        byte[] array = buffer.array();
        if (buffer.remaining() == array.length) {
          return array;
        }
        byte[] bytes = new byte[buffer.remaining()];
        System.arraycopy(array, buffer.arrayOffset() + buffer.position(), bytes, 0, buffer.remaining());
        return bytes;
      } else {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
      }
    }
    return buffer;
  }

  private Object readArray(Decoder decoder, Schema sourceSchema,
                             Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {

    int len = decoder.readInt();
    if (targetTypeToken.isArray()) {
      Object[] array = new Object[len];
      for (int i = 0; i < len; i++) {
        array[i] = read(decoder, sourceSchema.getComponentSchema(),
                        targetSchema.getComponentSchema(), targetTypeToken.getComponentType());
      }
      return array;
    } else if (Collection.class.isAssignableFrom(targetTypeToken.getRawType())) {
      try {
        Collection<Object> collection = (Collection<Object>)create(targetTypeToken);
        for (int i = 0; i < len; i++) {
          collection.add(read(decoder, sourceSchema.getComponentSchema(),
                        targetSchema.getComponentSchema(), targetTypeToken.getComponentType()));
        }
        return collection;
      } catch (Exception e) {
        throw propagate(e);
      }
    }
    throw new IOException("Only array or collection type is support for array value.");
  }

  private Map<Object, Object> readMap(Decoder decoder, Schema sourceSchema,
                                      Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {
    check(Map.class.isAssignableFrom(targetTypeToken.getRawType()), "Only map type is supported for map data.");
    Type type = targetTypeToken.getType();
    Preconditions.checkArgument(type instanceof ParameterizedType, "Only parameterized map is supported.");
    Type[] typeArgs = ((ParameterizedType) type).getActualTypeArguments();

    int len = decoder.readInt();
    Map<Object, Object> map = (Map<Object, Object>) create(targetTypeToken);
    for (int i = 0; i < len; i++) {
      Map.Entry<Schema, Schema> sourceEntry = sourceSchema.getMapSchema();
      Map.Entry<Schema, Schema> targetEntry = targetSchema.getMapSchema();

      map.put(read(decoder, sourceEntry.getKey(), targetEntry.getKey(), TypeToken.of(typeArgs[0])),
              read(decoder, sourceEntry.getValue(), targetEntry.getValue(), TypeToken.of(typeArgs[1])));
    }

    return map;
  }

  private Object readRecord(Decoder decoder, Schema sourceSchema,
                           Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {
    try {
      Object record = create(targetTypeToken);
      for (Schema.Field sourceField : sourceSchema.getFields()) {
        Schema.Field targetField = targetSchema.getField(sourceField.getName());
        if (targetField == null) {
          continue;
        }
        Field field = null;
        for (TypeToken<?> type : targetTypeToken.getTypes().classes()) {
          try {
            field = type.getRawType().getDeclaredField(sourceField.getName());
          } catch (NoSuchFieldException e) {
            // It's ok, keep searching.
            continue;
          }
          break;
        }
        check(field != null, "No such field in type. Type: %s, field: %s", targetTypeToken, sourceField.getName());
        if (!field.isAccessible()) {
          field.setAccessible(true);
        }
        TypeToken<?> fieldTypeToken = targetTypeToken.resolveType(field.getGenericType());
        field.set(record, read(decoder, sourceField.getSchema(), targetField.getSchema(), fieldTypeToken));
      }
      return record;
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  private Object readUnion(Decoder decoder, Schema sourceSchema,
                            Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {
    int idx = decoder.readInt();
    Schema sourceValueSchema = sourceSchema.getUnionSchemas().get(idx);
    List<Schema> targetUnion = targetSchema.getType() == Schema.Type.UNION ?
                                    targetSchema.getUnionSchemas() : ImmutableList.of(targetSchema);

    // A simple optimization to try resolve before resorting to linearly try the union schema.
    try {
      if (targetUnion.size() >= idx) {
        Schema targetValueSchema = targetUnion.get(idx);
        if (targetValueSchema.getType() == sourceValueSchema.getType()) {
          return read(decoder, sourceValueSchema, targetValueSchema, targetTypeToken);
        }
      }
    } catch (IOException e) {
      // It's ok to have exception here, and we'll go into union resolution logic
    }
    for (Schema targetValueSchema : targetUnion) {
      try {
        return read(decoder, sourceValueSchema, targetValueSchema, targetTypeToken);
      } catch (IOException e) {
        // It's ok to have exception here, as we'll keep trying until exhausted the target union.
      }
    }
    throw new IOException("Fail to resolve type against union. Schema: " + sourceValueSchema + ", union: " + targetUnion);
  }

  private Object resolveType(Decoder decoder, Schema.Type sourceType, Schema.Type targetType) throws IOException {
    switch (sourceType) {
      case BOOLEAN:
        switch (targetType) {
          case BOOLEAN:
            return decoder.readBool();
          case STRING:
            return String.valueOf(decoder.readBool());
        }
        break;
      case INT:
        switch (targetType) {
          case INT:
            return decoder.readInt();
          case LONG:
            return (long)decoder.readInt();
          case FLOAT:
            return (float)decoder.readInt();
          case DOUBLE:
            return (double)decoder.readInt();
          case STRING:
            return String.valueOf(decoder.readInt());
        }
        break;
      case LONG:
        switch (targetType) {
          case LONG:
            return decoder.readLong();
          case FLOAT:
            return (float)decoder.readLong();
          case DOUBLE:
            return (double)decoder.readLong();
          case STRING:
            return String.valueOf(decoder.readLong());
        }
        break;
      case FLOAT:
        switch (targetType) {
          case FLOAT:
            return decoder.readFloat();
          case DOUBLE:
            return (double)decoder.readFloat();
          case STRING:
            return String.valueOf(decoder.readFloat());
        }
        break;
      case DOUBLE:
        switch (targetType) {
          case DOUBLE:
            return decoder.readDouble();
          case STRING:
            return String.valueOf(decoder.readDouble());
        }
        break;
      case STRING:
        try {
          String value = decoder.readString();
          switch (targetType) {
            case STRING:
              return value;
            case BOOLEAN:
              return Boolean.valueOf(value);
            case INT:
              return Integer.valueOf(value);
            case LONG:
              return Long.valueOf(value);
            case FLOAT:
              return Float.valueOf(value);
            case DOUBLE:
              return Double.valueOf(value);
          }
        } catch (Exception e) {
          throw propagate(e);
        }
        break;
    }

    throw new IOException("Fail to resolve type " + sourceType + " to type " + targetType);
  }

  private void check(boolean condition, String message, Object...objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(message, objs));
    }
  }

  private IOException propagate(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException)t;
    }
    throw new IOException(t);
  }

  private Object create(TypeToken<?> type) {
    Class<?> rawType = type.getRawType();
    InstanceCreator<?> creator = creators.get(rawType);
    if (creator == null) {
      creator = creatorFactory.get(type);
      creators.put(rawType, creator);
    }
    return creator.create();
  }
}
