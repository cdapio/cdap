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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Decoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.SchemaHash;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A utility class for encoding/decoding {@link StreamEvent} into {@code byte[]} that is ready to be
 * used by QueueEntry.
 */
public final class StreamEventCodec {

  private static final Schema STREAM_EVENT_SCHEMA;

  static {
    Schema schema;
    try {
      schema = new ReflectionSchemaGenerator().generate(StreamEvent.class);
    } catch (UnsupportedTypeException e) {
      schema = null;
    }
    STREAM_EVENT_SCHEMA = schema;
  }

  /**
   * Encodes the given {@link StreamEvent} into {@code byte[]} that could become
   * payload of a QueueEntry.
   *
   * @param event The {@link StreamEvent} to encode.
   * @return Encoded {@code byte[]}.
   */
  public byte[] encodePayload(StreamEvent event) {
    // TODO: This is a bit hacky to do it directly for now, for performance reason.
    ByteBuffer body = event.getBody();
    Map<String, String> headers = event.getHeaders();
    long timestamp = event.getTimestamp();

    // Some assumption on the header size to minimize array copying
    // 16 bytes Schema hash + body size + (header size) * (50 bytes key/value pair) + 9 bytes timestamp (vlong encoding)
    ByteArrayOutputStream os = new ByteArrayOutputStream(16 + body.remaining() + headers.size() * 50 + 9);
    Encoder encoder = new BinaryEncoder(os);

    try {
      // Write the schema hash
      os.write(STREAM_EVENT_SCHEMA.getSchemaHash().toByteArray());

      StreamEventDataCodec.encode(event, encoder);
      encoder.writeLong(timestamp);
      return os.toByteArray();

    } catch (IOException e) {
      // It should never happens, otherwise something very wrong.
      throw Throwables.propagate(e);
    }
  }

  /**
   * Reverse of {@link #encodePayload(StreamEvent)}.
   *
   * @param payload The byte array containing the queue payload.
   * @return A {@link StreamEvent} reconstructed from payload.
   */
  public StreamEvent decodePayload(byte[] payload) {
    ByteBuffer buffer = ByteBuffer.wrap(payload);

    SchemaHash schemaHash = new SchemaHash(buffer);
    Preconditions.checkArgument(schemaHash.equals(STREAM_EVENT_SCHEMA.getSchemaHash()),
                                "Schema from payload not matching StreamEvent schema.");

    Decoder decoder = new BinaryDecoder(new ByteBufferInputStream(buffer));

    try {
      StreamEventData data = StreamEventDataCodec.decode(decoder);

      // Read the timestamp
      long timestamp = decoder.readLong();
      return new StreamEvent(data, timestamp);

    } catch (IOException e) {
      // It should never happens, otherwise something very wrong.
      throw Throwables.propagate(e);
    }
  }
}
