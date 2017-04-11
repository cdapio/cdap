/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.format.io;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.io.DatumWriter;
import co.cask.cdap.common.io.Encoder;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link DatumWriter} for writing out {@link StructuredRecord} to {@link Encoder}.
 */
public class StructuredRecordDatumWriter implements DatumWriter<StructuredRecord> {

  // Known Java type to schema type mapping
  // Doesn't have map and array as those need to use instanceof to check
  private static final Map<Class<?>, Schema.Type> TYPE_TO_SCHEMA = new IdentityHashMap<>(
    ImmutableMap.<Class<?>, Schema.Type>builder()
      .put(Boolean.class, Schema.Type.BOOLEAN)
      .put(Byte.class, Schema.Type.INT)
      .put(Short.class, Schema.Type.INT)
      .put(Integer.class, Schema.Type.INT)
      .put(Long.class, Schema.Type.LONG)
      .put(Float.class, Schema.Type.FLOAT)
      .put(Double.class, Schema.Type.DOUBLE)
      .put(String.class, Schema.Type.STRING)
      .put(ByteBuffer.class, Schema.Type.BYTES)
      .put(byte[].class, Schema.Type.BYTES)
      .put(StructuredRecord.class, Schema.Type.RECORD)
      .build()
  );

  @Override
  public void encode(StructuredRecord data, Encoder encoder) throws IOException {
    encode(encoder, data.getSchema(), data);
  }

  /**
   * Encodes a value based on a {@link Schema}
   *
   * @param encoder the encoder to use
   * @param schema the {@link Schema} of the value
   * @param value the value to encode
   * @throws IOException If failed to encode
   */
  protected final void encode(Encoder encoder, Schema schema, Object value) throws IOException {
    switch (schema.getType()) {
      case NULL:
        encoder.writeNull();
        break;
      case BOOLEAN:
        encoder.writeBool((Boolean) value);
        break;
      case INT:
        encoder.writeInt((Integer) value);
        break;
      case LONG:
        encoder.writeLong((Long) value);
        break;
      case FLOAT:
        encoder.writeFloat((Float) value);
        break;
      case DOUBLE:
        encoder.writeDouble((Double) value);
        break;
      case BYTES:
        encodeBytes(encoder, value);
        break;
      case STRING:
        encoder.writeString((String) value);
        break;
      case ENUM:
        encodeEnum(encoder, schema, value);
        break;
      case ARRAY:
        encodeArray(encoder, schema.getComponentSchema(), value);
        break;
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        encodeMap(encoder, mapSchema.getKey(), mapSchema.getValue(), value);
        break;
      case RECORD:
        encodeRecord(encoder, schema, value);
        break;
      case UNION:
        encodeUnion(encoder, schema, findUnionSchema(schema, value), value);
        break;
    }
  }

  /**
   * Encodes a enum value. This method will encode with the enum index of the enum value according to the Schema.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param enumSchema The {@link Schema} for the enum field
   * @param value The enum value
   * @throws IOException If failed to encode
   */
  protected void encodeEnum(Encoder encoder, Schema enumSchema, Object value) throws IOException {
    String enumValue = value instanceof Enum ? ((Enum) value).name() : value.toString();
    encoder.writeInt(enumSchema.getEnumIndex(enumValue));
  }

  /**
   * Encodes the beginning of an array. This method will encode with the size of the array.
   * Sub-class can override this to have different behavior
   *
   * @param encoder The encoder to use
   * @param elementSchema The {@link Schema} of the array element
   * @param size Number of array elements
   * @throws IOException If failed to encode
   */
  protected void encodeArrayBegin(Encoder encoder, Schema elementSchema, int size) throws IOException {
    encoder.writeInt(size);
  }

  /**
   * Encodes an element of an array. This method will encode the element by calling the
   * {@link #encode(Encoder, Schema, Object)} method with the element schema.
   *
   * @param encoder The encoder to use
   * @param elementSchema The {@link Schema} of the array element
   * @param element the value to encode
   * @throws IOException If failed to encode
   */
  protected void encodeArrayElement(Encoder encoder, Schema elementSchema, Object element) throws IOException {
    encode(encoder, elementSchema, element);
  }

  /**
   * Encodes the ending of an array. This method writes out {@code 0} to signal the end of the array.
   * Sub-class can override this to have different behavior
   *
   * @param encoder The encoder to use
   * @param elementSchema The {@link Schema} of the array element
   * @param size Number of array elements
   * @throws IOException If failed to encode
   */
  protected void encodeArrayEnd(Encoder encoder, Schema elementSchema, int size) throws IOException {
    encoder.writeInt(0);
  }

  /**
   * Encodes the beginning of a {@link Map}. This method will write out the size of the map.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param keySchema The {@link Schema} of the map key
   * @param valueSchema The {@link Schema} of the map value
   * @param size size of the map
   * @throws IOException If failed to encode
   */
  protected void encodeMapBegin(Encoder encoder, Schema keySchema, Schema valueSchema, int size) throws IOException {
    encoder.writeInt(size);
  }

  /**
   * Encodes a map entry. This method will write out the key based on the key schema, followed by the value based on
   * the value schema by calling the {@link #encode(Encoder, Schema, Object)} method.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param keySchema The {@link Schema} of the map key
   * @param valueSchema The {@link Schema} of the map value
   * @param entry the map entry to encode
   * @throws IOException If failed to encode
   */
  protected void encodeMapEntry(Encoder encoder, Schema keySchema,
                                Schema valueSchema, Map.Entry<?, ?> entry) throws IOException {
    encode(encoder, keySchema, entry.getKey());
    encode(encoder, valueSchema, entry.getValue());
  }

  /**
   * Encodes the ending of a {@link Map}. This method writes out {@code 0} to signal the end of the map.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param keySchema The {@link Schema} of the map key
   * @param valueSchema The {@link Schema} of the map value
   * @param size size of the map
   * @throws IOException If failed to encode
   */
  protected void encodeMapEnd(Encoder encoder, Schema keySchema, Schema valueSchema, int size) throws IOException {
    encoder.writeInt(0);
  }

  /**
   * Encodes the beginning of record. This method is an no-op.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param schema The {@link Schema} of the record
   * @throws IOException If failed to encode
   */
  protected void encodeRecordBegin(Encoder encoder, Schema schema) throws IOException {
    // no-op
  }

  /**
   * Encodes a record field. This method will encode the field based on the field schema by calling the
   * {@link #encode(Encoder, Schema, Object)} method.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param field The {@link Schema.Field} to encode
   * @param value the field value to encode
   * @throws IOException If failed to encode
   */
  protected void encodeRecordField(Encoder encoder, Schema.Field field, Object value) throws IOException {
    encode(encoder, field.getSchema(), value);
  }

  /**
   * Encodes the ending of record. This method is an no-op.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param schema The {@link Schema} of the record
   * @throws IOException If failed to encode
   */
  protected void encodeRecordEnd(Encoder encoder, Schema schema) throws IOException {
    // no-op
  }

  /**
   * Encodes a union value. This method will write out the index of the matching schema in the union schema,
   * followed by encoding the value.
   * Sub-class can override this to have different behavior.
   *
   * @param encoder The encoder to use
   * @param schema The union schema
   * @param matchingIdx The index in the union schema that matches the data type
   * @param value The value to encode
   * @throws IOException If failed to encode
   */
  protected void encodeUnion(Encoder encoder, Schema schema, int matchingIdx, Object value) throws IOException {
    encoder.writeInt(matchingIdx);
    encode(encoder, schema.getUnionSchema(matchingIdx), value);
  }

  private void encodeArray(Encoder encoder, Schema elementSchema, Object array) throws IOException {
    if (!(array instanceof Collection) && !array.getClass().isArray()) {
      throw new IOException("Expects either Collection or array. Got: " + array.getClass());
    }

    int size = array instanceof Collection ? ((Collection) array).size() : Array.getLength(array);

    encodeArrayBegin(encoder, elementSchema, size);
    if (array instanceof Collection) {
      for (Object element : (Collection) array) {
        encodeArrayElement(encoder, elementSchema, element);
      }
    } else {
      for (int i = 0; i < size; i++) {
        encode(encoder, elementSchema, Array.get(array, i));
      }
    }
    encodeArrayEnd(encoder, elementSchema, size);
  }

  private void encodeMap(Encoder encoder, Schema keySchema, Schema valueSchema, Object map) throws IOException {
    if (!(map instanceof Map)) {
      throw new IOException("Expects Map type. Got: " + map.getClass());
    }

    int size = ((Map<?, ?>) map).size();
    encodeMapBegin(encoder, keySchema, valueSchema, size);
    for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
      encodeMapEntry(encoder, keySchema, valueSchema, entry);
    }
    encodeMapEnd(encoder, keySchema, valueSchema, size);
  }

  private void encodeRecord(Encoder encoder, Schema recordSchema, Object record) throws IOException {
    if (!(record instanceof StructuredRecord)) {
      throw new IOException("Expected StructuredRecord type. Got: " + record.getClass());
    }

    encodeRecordBegin(encoder, recordSchema);
    for (Schema.Field field : recordSchema.getFields()) {
      encodeRecordField(encoder, field, ((StructuredRecord) record).get(field.getName()));
    }
    encodeRecordEnd(encoder, recordSchema);
  }

  private int findUnionSchema(Schema unionSchema, @Nullable Object value) throws IOException {
    Schema.Type type = getSchemaType(value);

    int idx = 0;
    for (Schema schema : unionSchema.getUnionSchemas()) {
      // Just match the type, not matching the detail schema as it'd be too expensive.
      if (schema.getType() == type) {
        return idx;
      }
      idx++;
    }
    throw new IOException("Value type " + type + " not valid in union: " + unionSchema);
  }

  private void encodeBytes(Encoder encoder, Object value) throws IOException {
    if (value instanceof ByteBuffer) {
      encodeBytes(encoder, (ByteBuffer) value);
    } else if (value.getClass().isArray() && value.getClass().getComponentType().equals(byte.class)) {
      byte[] bytes = (byte[]) value;
      encoder.writeBytes(bytes, 0, bytes.length);
    } else {
      throw new IOException("Expects either ByteBuffer or byte[]. Got " + value.getClass());
    }
  }

  private void encodeBytes(Encoder encoder, ByteBuffer buffer) throws IOException {
    if (buffer.hasArray()) {
      encoder.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    } else {
      byte[] buf = Bytes.getBytes(buffer);
      buffer.mark();
      buffer.get(buf);
      buffer.reset();
      encoder.writeBytes(buf, 0, buf.length);
    }
  }

  private Schema.Type getSchemaType(@Nullable Object value) throws IOException {
    if (value == null) {
      return Schema.Type.NULL;
    }

    Class<?> cls = value.getClass();
    Schema.Type type = TYPE_TO_SCHEMA.get(cls);
    if (type != null) {
      return type;
    }

    if (Collection.class.isAssignableFrom(cls) || cls.isArray()) {
      return Schema.Type.ARRAY;
    }

    if (Map.class.isAssignableFrom(cls)) {
      return Schema.Type.MAP;
    }

    throw new IOException("Unsupported type found in StructuredRecord: " + cls);
  }
}
