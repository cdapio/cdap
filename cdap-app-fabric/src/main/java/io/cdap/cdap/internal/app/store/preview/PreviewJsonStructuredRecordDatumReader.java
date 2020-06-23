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
import io.cdap.cdap.common.io.Decoder;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;


/**
 *
 */
public class PreviewJsonStructuredRecordDatumReader extends JsonStructuredRecordDatumReader {

  @Override
  protected Object decode(Decoder decoder, Schema schema) throws IOException {
    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DECIMAL:
          String s = decoder.readString();
          return new BigDecimal(s);
      }
    }
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
}
