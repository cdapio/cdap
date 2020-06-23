/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.preview;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.io.Encoder;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

/**
 *
 */
public class PreviewJsonStructuredRecordDatumWriter extends JsonStructuredRecordDatumWriter {

  @Override
  protected void encode(Encoder encoder, Schema schema, Object value) throws IOException {
    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DECIMAL:
          int scale = schema.getScale();
          BigDecimal bigDecimal = new BigDecimal(new BigInteger((byte[]) value), scale);
          encoder.writeString(bigDecimal.toPlainString());
          return;
      }
    }

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
}
