package com.continuuity.internal.io;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.lang.Instantiator;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Reflection based Datnum Reader.
 *
 * @param <T> type T reader
 */
public final class ReflectionDatumReader<T> implements DatumReader<T> {

  private final Schema schema;
  private final TypeToken<T> type;
  private final Map<Class<?>, Instantiator<?>> creators;
  private final InstantiatorFactory creatorFactory;
  private final FieldAccessorFactory fieldAccessorFactory;

  @SuppressWarnings("unchecked")
  public ReflectionDatumReader(Schema schema, TypeToken<T> type) {
    this.schema = schema;

    // TODO: (ENG-2940) This is a hack. Until we support class generation based on interface
    this.type = type.getRawType().equals(StreamEvent.class)
                      ? (TypeToken<T>) TypeToken.of(DefaultStreamEvent.class) : type;

    this.creatorFactory = new InstantiatorFactory(true);
    this.creators = Maps.newIdentityHashMap();
    this.fieldAccessorFactory = new ReflectionFieldAccessorFactory();
  }

  @SuppressWarnings("unchecked")
  @Override
  public T read(Decoder decoder, Schema sourceSchema) throws IOException {
    return (T) read(decoder, sourceSchema, schema, type);
  }

  private Object read(Decoder decoder, Schema sourceSchema,
                      Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {

    if (sourceSchema.getType() != Schema.Type.UNION && targetSchema.getType() == Schema.Type.UNION) {
      // Try every target schemas
      for (Schema schema : targetSchema.getUnionSchemas()) {
        try {
          return doRead(decoder, sourceSchema, schema, targetTypeToken);
        } catch (IOException e) {
          // Continue;
        }
      }
      throw new IOException(String.format("No matching schema to resolve %s to %s", sourceSchema, targetSchema));
    }
    return doRead(decoder, sourceSchema, targetSchema, targetTypeToken);
  }

  private Object doRead(Decoder decoder, Schema sourceSchema,
                        Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {

    Schema.Type sourceType = sourceSchema.getType();
    Schema.Type targetType = targetSchema.getType();

    switch(sourceType) {
      case NULL:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return decoder.readNull();
      case BYTES:
        check(sourceType == targetType, "Fails to resolve %s to %s", sourceType, targetType);
        return readBytes(decoder, targetTypeToken);
      case ENUM:
        String enumValue = sourceSchema.getEnumValue(decoder.readInt());
        check(targetSchema.getEnumValues().contains(enumValue), "Enum value '%s' missing in target.", enumValue);
        try {
          return targetTypeToken.getRawType().getMethod("valueOf", String.class).invoke(null, enumValue);
        } catch (Exception e) {
          throw new IOException(e);
        }
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
      return resolveType(decoder, sourceType, targetType, targetTypeToken);
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
    } else if (targetTypeToken.getRawType().equals(UUID.class) && buffer.remaining() == Longs.BYTES * 2) {
      return new UUID(buffer.getLong(), buffer.getLong());
    }
    return buffer;
  }

  @SuppressWarnings("unchecked")
  private Object readArray(Decoder decoder, Schema sourceSchema,
                           Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {

    TypeToken<?> componentType = null;
    if (targetTypeToken.isArray()) {
      componentType = targetTypeToken.getComponentType();
    } else if (Collection.class.isAssignableFrom(targetTypeToken.getRawType())) {
      Type type = targetTypeToken.getType();
      check(type instanceof ParameterizedType, "Only parameterized type is supported for collection.");
      componentType = TypeToken.of(((ParameterizedType) type).getActualTypeArguments()[0]);
    }
    check(componentType != null, "Only array or collection type is support for array value.");

    int len = decoder.readInt();
    Collection<Object> collection = (Collection<Object>) create(targetTypeToken);
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        collection.add(read(decoder, sourceSchema.getComponentSchema(),
                            targetSchema.getComponentSchema(), componentType)
        );
      }
      len = decoder.readInt();
    }

    if (targetTypeToken.isArray()) {
      Object array = Array.newInstance(targetTypeToken.getComponentType().getRawType(), collection.size());
      int idx = 0;
      for (Object obj : collection) {
        Array.set(array, idx++, obj);
      }
      return array;
    }
    return collection;
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> readMap(Decoder decoder, Schema sourceSchema,
                                      Schema targetSchema, TypeToken<?> targetTypeToken) throws IOException {
    check(Map.class.isAssignableFrom(targetTypeToken.getRawType()), "Only map type is supported for map data.");
    Type type = targetTypeToken.getType();
    Preconditions.checkArgument(type instanceof ParameterizedType, "Only parameterized map is supported.");
    Type[] typeArgs = ((ParameterizedType) type).getActualTypeArguments();

    int len = decoder.readInt();
    Map<Object, Object> map = (Map<Object, Object>) create(targetTypeToken);
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        Map.Entry<Schema, Schema> sourceEntry = sourceSchema.getMapSchema();
        Map.Entry<Schema, Schema> targetEntry = targetSchema.getMapSchema();

        map.put(read(decoder, sourceEntry.getKey(), targetEntry.getKey(), TypeToken.of(typeArgs[0])),
                read(decoder, sourceEntry.getValue(), targetEntry.getValue(), TypeToken.of(typeArgs[1])));
      }
      len = decoder.readInt();
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
          skip(decoder, sourceField.getSchema());
          continue;
        }
        FieldAccessor fieldAccessor = fieldAccessorFactory.getFieldAccessor(targetTypeToken, sourceField.getName());
        fieldAccessor.set(record,
                          read(decoder, sourceField.getSchema(), targetField.getSchema(), fieldAccessor.getType()));
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


    if (targetSchema.getType() == Schema.Type.UNION) {
      try {
        // A simple optimization to try resolve before resorting to linearly try the union schema.
        Schema targetValueSchema = targetSchema.getUnionSchema(idx);
        if (targetValueSchema != null && targetValueSchema.getType() == sourceValueSchema.getType()) {
          return read(decoder, sourceValueSchema, targetValueSchema, targetTypeToken);
        }
      } catch (IOException e) {
        // OK to ignore it, as we'll do union schema resolution
      }
      for (Schema targetValueSchema : targetSchema.getUnionSchemas()) {
        try {
          return read(decoder, sourceValueSchema, targetValueSchema, targetTypeToken);
        } catch (IOException e) {
          // It's ok to have exception here, as we'll keep trying until exhausted the target union.
        }
      }
      throw new IOException(String.format("Fail to resolve %s to %s", sourceSchema, targetSchema));
    } else {
      return read(decoder, sourceValueSchema, targetSchema, targetTypeToken);
    }
  }

  private void skip(Decoder decoder, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        break;
      case BOOLEAN:
        decoder.readBool();
        break;
      case INT:
        decoder.readInt();
        break;
      case LONG:
        decoder.readLong();
        break;
      case FLOAT:
        decoder.skipFloat();
        break;
      case DOUBLE:
        decoder.skipDouble();
        break;
      case BYTES:
        decoder.skipBytes();
        break;
      case STRING:
        decoder.skipString();
        break;
      case ENUM:
        decoder.readInt();
        break;
      case ARRAY:
        skipArray(decoder, schema.getComponentSchema());
        break;
      case MAP:
        skipMap(decoder, schema.getMapSchema());
        break;
      case RECORD:
        skipRecord(decoder, schema);
        break;
      case UNION:
        skip(decoder, schema.getUnionSchema(decoder.readInt()));
        break;
    }
  }

  private void skipArray(Decoder decoder, Schema componentSchema) throws IOException {
    int len = decoder.readInt();
    while (len != 0) {
      skip(decoder, componentSchema);
      len = decoder.readInt();
    }
  }

  private void skipMap(Decoder decoder, Map.Entry<Schema, Schema> mapSchema) throws IOException {
    int len = decoder.readInt();
    while (len != 0) {
      skip(decoder, mapSchema.getKey());
      skip(decoder, mapSchema.getValue());
      len = decoder.readInt();
    }
  }

  private void skipRecord(Decoder decoder, Schema recordSchema) throws IOException {
    for (Schema.Field field : recordSchema.getFields()) {
      skip(decoder, field.getSchema());
    }
  }

  private Object resolveType(Decoder decoder, Schema.Type sourceType,
                             Schema.Type targetType, TypeToken<?> targetTypeToken) throws IOException {
    switch(sourceType) {
      case BOOLEAN:
        switch(targetType) {
          case BOOLEAN:
            return decoder.readBool();
          case STRING:
            return String.valueOf(decoder.readBool());
        }
        break;
      case INT:
        switch(targetType) {
          case INT:
            Class<?> targetClass = targetTypeToken.getRawType();
            int value = decoder.readInt();
            if (targetClass.equals(byte.class) || targetClass.equals(Byte.class)) {
              return (byte) value;
            }
            if (targetClass.equals(char.class) || targetClass.equals(Character.class)) {
              return (char) value;
            }
            if (targetClass.equals(short.class) || targetClass.equals(Short.class)) {
              return (short) value;
            }
            return value;
          case LONG:
            return (long) decoder.readInt();
          case FLOAT:
            return (float) decoder.readInt();
          case DOUBLE:
            return (double) decoder.readInt();
          case STRING:
            return String.valueOf(decoder.readInt());
        }
        break;
      case LONG:
        switch(targetType) {
          case LONG:
            return decoder.readLong();
          case FLOAT:
            return (float) decoder.readLong();
          case DOUBLE:
            return (double) decoder.readLong();
          case STRING:
            return String.valueOf(decoder.readLong());
        }
        break;
      case FLOAT:
        switch(targetType) {
          case FLOAT:
            return decoder.readFloat();
          case DOUBLE:
            return (double) decoder.readFloat();
          case STRING:
            return String.valueOf(decoder.readFloat());
        }
        break;
      case DOUBLE:
        switch(targetType) {
          case DOUBLE:
            return decoder.readDouble();
          case STRING:
            return String.valueOf(decoder.readDouble());
        }
        break;
      case STRING:
        switch(targetType) {
          case STRING:
            String str = decoder.readString();
            Class<?> targetClass = targetTypeToken.getRawType();
            if (targetClass.equals(URI.class)) {
              return URI.create(str);
            } else if (targetClass.equals(URL.class)) {
              return new URL(str);
            }
            return str;
        }
        break;
    }

    throw new IOException("Fail to resolve type " + sourceType + " to type " + targetType);
  }

  private void check(boolean condition, String message, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(message, objs));
    }
  }

  private IOException propagate(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException) t;
    }
    throw new IOException(t);
  }

  private Object create(TypeToken<?> type) {
    Class<?> rawType = type.getRawType();
    Instantiator<?> creator = creators.get(rawType);
    if (creator == null) {
      creator = creatorFactory.get(type);
      creators.put(rawType, creator);
    }
    return creator.create();
  }
}
