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
import co.cask.cdap.common.io.Encoder;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link StructuredRecordDatumWriter} for encoding {@link StructuredRecord} to json.
 */
public final class JsonStructuredRecordDatumWriter extends StructuredRecordDatumWriter {

  @Override
  public void encode(StructuredRecord data, Encoder encoder) throws IOException {
    if (!(encoder instanceof JsonEncoder)) {
      throw new IOException("The JsonStructuredRecordDatumWriter can only encode using a JsonEncoder");
    }
    super.encode(data, encoder);
  }

  @Override
  protected void encodeEnum(Encoder encoder, Schema enumSchema, Object value) throws IOException {
    String enumValue = value instanceof Enum ? ((Enum) value).name() : value.toString();
    encoder.writeString(enumValue);
  }

  @Override
  protected void encodeArrayBegin(Encoder encoder, Schema elementSchema, int size) throws IOException {
    getJsonWriter(encoder).beginArray();
  }

  @Override
  protected void encodeArrayEnd(Encoder encoder, Schema elementSchema, int size) throws IOException {
    getJsonWriter(encoder).endArray();
  }

  @Override
  protected void encodeMapBegin(Encoder encoder, Schema keySchema, Schema valueSchema, int size) throws IOException {
    if (!keySchema.isCompatible(Schema.of(Schema.Type.STRING))) {
      throw new IOException("Complex key type not supported: " + keySchema);
    }
    getJsonWriter(encoder).beginObject();
  }

  @Override
  protected void encodeMapEntry(Encoder encoder, Schema keySchema,
                                Schema valueSchema, Map.Entry<?, ?> entry) throws IOException {
    getJsonWriter(encoder).name(entry.getKey().toString());
    encode(encoder, valueSchema, entry.getValue());
  }

  @Override
  protected void encodeMapEnd(Encoder encoder, Schema keySchema, Schema valueSchema, int size) throws IOException {
    getJsonWriter(encoder).endObject();
  }

  @Override
  protected void encodeRecordBegin(Encoder encoder, Schema schema) throws IOException {
    getJsonWriter(encoder).beginObject();
  }

  @Override
  protected void encodeRecordField(Encoder encoder, Schema.Field field, Object value) throws IOException {
    getJsonWriter(encoder).name(field.getName());
    encode(encoder, field.getSchema(), value);
  }

  @Override
  protected void encodeRecordEnd(Encoder encoder, Schema schema) throws IOException {
    getJsonWriter(encoder).endObject();
  }

  @Override
  protected void encodeUnion(Encoder encoder, Schema schema, int matchingIdx, Object value) throws IOException {
    encode(encoder, schema.getUnionSchema(matchingIdx), value);
  }

  private JsonWriter getJsonWriter(Encoder encoder) {
    // Type already checked in the encode method, hence assuming the casting is fine.
    return ((JsonEncoder) encoder).getJsonWriter();
  }
}
