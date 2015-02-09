/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.io.Encoder;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * A {@link DatumWriter} that uses java reflection to encode data. The encoding schema it uses is
 * the same as the binary encoding as specified in Avro, with the enhancement of support for non-string
 * map keys.
 *
 * @param <T> Type T to be written.
 */
public final class ReflectionDatumWriter<T> implements DatumWriter<T> {

  private final Schema schema;

  public ReflectionDatumWriter(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public void encode(T data, Encoder encoder) throws IOException {
    ReflectionWriter writer = new EncoderReflectionWriter(encoder);
    writer.write(data, schema);
  }

  /**
   * Uses an {@link Encoder} to write an object as binary encoded Avro, with the enhancement of support for
   * non-string map keys.
   */
  private static class EncoderReflectionWriter extends ReflectionWriter {
    private final Encoder encoder;

    private EncoderReflectionWriter(Encoder encoder) {
      this.encoder = encoder;
    }

    @Override
    protected void writeNull(String name) throws IOException {
      encoder.writeNull();
    }

    @Override
    protected void writeBool(String name, Boolean val) throws IOException {
      encoder.writeBool(val);
    }

    @Override
    protected void writeInt(String name, int val) throws IOException {
      encoder.writeInt(val);
    }

    @Override
    protected void writeLong(String name, long val) throws IOException {
      encoder.writeLong(val);
    }

    @Override
    protected void writeFloat(String name, Float val) throws IOException {
      encoder.writeFloat(val);
    }

    @Override
    protected void writeDouble(String name, Double val) throws IOException {
      encoder.writeDouble(val);
    }

    @Override
    protected void writeString(String name, String val) throws IOException {
      encoder.writeString(val);
    }

    @Override
    protected void writeBytes(String name, ByteBuffer val) throws IOException {
      encoder.writeBytes(val);
    }

    @Override
    protected void writeBytes(String name, byte[] val) throws IOException {
      encoder.writeBytes(val);
    }

    @Override
    protected void writeEnum(String name, String val, Schema schema) throws IOException {
      int idx = schema.getEnumIndex(val);
      if (idx < 0) {
        throw new IOException("Invalid enum value " + val);
      }
      encoder.writeInt(idx);
    }

    @Override
    protected void writeArray(String name, Collection col, Schema componentSchema) throws IOException {
      int size = col.size();
      encoder.writeInt(size);
      for (Object obj : col) {
        write(null, obj, componentSchema);
      }
      if (size > 0) {
        encoder.writeInt(0);
      }
    }

    @Override
    protected void writeArray(String name, Object array, Schema componentSchema) throws IOException {
      int size = Array.getLength(array);
      encoder.writeInt(size);
      for (int i = 0; i < size; i++) {
        write(null, Array.get(array, i), componentSchema);
      }
      if (size > 0) {
        encoder.writeInt(0);
      }
    }

    @Override
    protected void writeMap(String name, Map<?, ?> map,
                            Map.Entry<Schema, Schema> mapSchema) throws IOException {
      int size = map.size();
      encoder.writeInt(size);
      Schema keySchema = mapSchema.getKey();
      Schema valSchema = mapSchema.getValue();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        write(null, entry.getKey(), keySchema);
        write(null, entry.getValue(), valSchema);
      }
      if (size > 0) {
        encoder.writeInt(0);
      }
    }

    @Override
    protected void writeUnion(String name, Object val, Schema schema) throws IOException {
      // Assumption in schema generation that index 0 is the object type, index 1 is null.
      if (val == null) {
        encoder.writeInt(1);
      } else {
        seenRefs.remove(val);
        encoder.writeInt(0);
        write(name, val, schema.getUnionSchema(0));
      }
    }
  }
}
