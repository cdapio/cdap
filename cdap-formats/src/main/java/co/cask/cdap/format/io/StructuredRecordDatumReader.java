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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.io.DatumReader;
import co.cask.cdap.common.io.Decoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link DatumReader} for reading {@link StructuredRecord}.
 */
public class StructuredRecordDatumReader implements DatumReader<StructuredRecord> {

  @Override
  public StructuredRecord read(Decoder decoder, Schema sourceSchema) throws IOException {
    if (sourceSchema.getType() != Schema.Type.RECORD) {
      throw new IOException("Expected schema of type RECORD. Got: " + sourceSchema.getType());
    }
    return decodeRecord(decoder, sourceSchema);
  }

  /**
   * Decodes a value based on the {@link Schema}.
   *
   * @param decoder The decoder to decode value from
   * @param schema The schema of the value
   * @return The decoded value
   * @throws IOException If failed to decode
   */
  protected final Object decode(Decoder decoder, Schema schema) throws IOException {
    switch (schema.getType()) {
      case NULL:
        decoder.readNull();
        return null;
      case BOOLEAN:
        return decoder.readBool();
      case INT:
        return decoder.readInt();
      case LONG:
        return decoder.readLong();
      case FLOAT:
        return decoder.readFloat();
      case DOUBLE:
        return decoder.readDouble();
      case BYTES:
        return decoder.readBytes();
      case STRING:
        return decoder.readString();
      case ENUM:
        return decodeEnum(decoder, schema);
      case ARRAY:
        return decodeArray(decoder, schema.getComponentSchema());
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        return decodeMap(decoder, mapSchema.getKey(), mapSchema.getValue());
      case RECORD:
        return decodeRecord(decoder, schema);
      case UNION:
        return decodeUnion(decoder, schema);
    }

    throw new IOException("Unsupported schema: " + schema);
  }

  /**
   * Decodes a enum value based on the given {@link Schema}. This method reads an integer as the index in the
   * enum schema.
   * Sub-class can override this to have different behavior.
   *
   * @param decoder The decode to decode value from
   * @param schema The {@link Schema} of the enum
   * @return A {@link String} representation of the enum value
   * @throws IOException If failed to decode
   */
  protected String decodeEnum(Decoder decoder, Schema schema) throws IOException {
    return schema.getEnumValue(decoder.readInt());
  }

  /**
   * Decodes an array with array element of a given {@link Schema}. This method reads an integer as the size of
   * a chunk, followed by reading that many array element using the element schema, following by reading the size
   * of the next chunk, until it reads {@code 0}.
   * Sub-class can override this to have different behavior.
   *
   * @param decoder The decode to decode value from
   * @param elementSchema The {@link Schema} of the array elements
   * @return A {@link Collection} containing all array elements
   * @throws IOException If failed to decode
   */
  protected Collection<?> decodeArray(Decoder decoder, Schema elementSchema) throws IOException {
    int size = decoder.readInt();
    List<Object> array = new ArrayList<>(size);

    while (size != 0) {
      for (int i = 0; i < size; i++) {
        array.add(decode(decoder, elementSchema));
      }
      size = decoder.readInt();
    }

    return array;
  }

  /**
   * Decodes a map. This method reads an integer as the size of a chunk, followed by reading that many
   * key and value pairs using the key and value schema, following by reading the size
   * of the next chunk, until it reads {@code 0}.
   * Sub-class can override this to have different behavior.
   *
   * @param decoder The decode to decode value from
   * @param keySchema The {@link Schema} of the map key
   * @param valueSchema The {@link Schema} of the map value
   * @return A {@link Map} containing all map entries
   * @throws IOException If failed to decode
   */
  protected Map<?, ?> decodeMap(Decoder decoder, Schema keySchema, Schema valueSchema) throws IOException {
    int size = decoder.readInt();
    Map<Object, Object> map = new LinkedHashMap<>(size);

    while (size != 0) {
      for (int i = 0; i < size; i++) {
        map.put(decode(decoder, keySchema), decode(decoder, valueSchema));
      }
      size = decoder.readInt();
    }

    return map;
  }

  /**
   * Decodes a record. This method decode fields one by one based on the given record schema.
   * Sub-class can override this to have different behavior.
   *
   * @param decoder The decode to decode value from
   * @param schema The {@link Schema} of the record
   * @return A {@link StructuredRecord} representing the record decoded
   * @throws IOException If failed to decode
   */
  protected StructuredRecord decodeRecord(Decoder decoder, Schema schema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field.getName(), decode(decoder, field.getSchema()));
    }
    return builder.build();
  }

  /**
   * Decodes a value from a union. This method first decode an integer as the index in the union schema for the
   * value schema, followed decoding the value using that schema.
   *
   * @param decoder The decode to decode value from
   * @param schema The {@link Schema} of the union
   * @return The value decoded
   * @throws IOException If failed to decode
   */
  protected Object decodeUnion(Decoder decoder, Schema schema) throws IOException {
    return decode(decoder, schema.getUnionSchema(decoder.readInt()));
  }
}
