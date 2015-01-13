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
package co.cask.cdap.common.stream;

import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.common.io.Decoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Utility class for encode/decode {@link co.cask.cdap.api.stream.StreamEventData}.
 *
 * The StreamEventData is encoded using Avro binary encoding, with the schema:
 *
 * <pre>
 * {
 *   "type": "record",
 *   "name": "StreamEvent",
 *   "fields" : [
 *     {"name": "body", "type": "bytes"},
 *     {"name": "headers", "type": {"type": "map", "values": ["string", "null"]}}
 *   ]
 * }
 * </pre>
 *
 */
public final class StreamEventDataCodec {

  public static final Schema STREAM_DATA_SCHEMA;

  static {
    Schema schema;
    try {
      schema = new ReflectionSchemaGenerator().generate(StreamEventData.class);
    } catch (UnsupportedTypeException e) {
      // Never happen, as schema can always be generated from StreamEventData.
      schema = null;
    }
    STREAM_DATA_SCHEMA = schema;
  }


  /**
   * Encodes the given {@link co.cask.cdap.api.stream.StreamEventData} using the {@link Encoder}.
   *
   * @param data The data to encode
   * @param encoder The encoder
   * @throws IOException If there is any IO error during encoding.
   */
  public static void encode(StreamEventData data, Encoder encoder) throws IOException {
    // The schema is sorted by name, hence it is {body, header}.
    // Writes the body
    encoder.writeBytes(data.getBody());

    // Writes the headers
    Map<String, String> headers = data.getHeaders();
    encoder.writeInt(headers.size());
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      String value = entry.getValue();
      encoder.writeString(entry.getKey())
             .writeInt(value == null ? 1 : 0)
             .writeString(entry.getValue());
    }
    if (!headers.isEmpty()) {
      encoder.writeInt(0);
    }
  }


  /**
   * Decodes from the given {@link Decoder} to reconstruct a {@link co.cask.cdap.api.stream.StreamEventData}.
   *
   * @param decoder Decoder to read data from.
   * @return A new instance of {@link co.cask.cdap.api.stream.StreamEventData}.
   * @throws IOException If there is any IO error during decoding.
   */
  public static StreamEventData decode(Decoder decoder) throws IOException {
    // Reads the body
    ByteBuffer body = decoder.readBytes();

    // Reads the headers
    ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
    int len = decoder.readInt();
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        String key = decoder.readString();
        String value = decoder.readInt() == 0 ? decoder.readString() : (String) decoder.readNull();
        headers.put(key, value);
      }
      len = decoder.readInt();
    }
    return new StreamEventData(headers.build(), body);
  }

  /**
   * Skips an encoded {@link co.cask.cdap.api.stream.StreamEventData}.
   *
   * @param decoder Decoder to skip data from.
   * @throws IOException If there is any IO error during decoding.
   */
  public static void skip(Decoder decoder) throws IOException {
    // Skips the body
    decoder.skipBytes();

    // Skips the headers
    int len = decoder.readInt();
    while (len != 0) {
      for (int i = 0; i < len; i++) {
        decoder.skipString();
        if (decoder.readInt() == 0) {
          decoder.skipString();
        } else {
          decoder.readNull();
        }
      }
      len = decoder.readInt();
    }
  }

  private StreamEventDataCodec() {
  }
}
