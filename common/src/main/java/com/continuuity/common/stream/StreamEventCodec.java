package com.continuuity.common.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaHash;
import com.continuuity.internal.io.UnsupportedTypeException;
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
      return new DefaultStreamEvent(data, timestamp);

    } catch (IOException e) {
      // It should never happens, otherwise something very wrong.
      throw Throwables.propagate(e);
    }
  }
}
