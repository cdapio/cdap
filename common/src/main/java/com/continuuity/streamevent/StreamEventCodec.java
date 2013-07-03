package com.continuuity.streamevent;

import com.continuuity.api.flow.flowlet.StreamEvent;
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
import com.google.common.collect.ImmutableMap;

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

    // Some assumption on the header size to minimize array copying
    ByteArrayOutputStream os = new ByteArrayOutputStream(16 + body.remaining() + headers.size() * 50);
    Encoder encoder = new BinaryEncoder(os);

    try {
      // Write the schema hash
      os.write(STREAM_EVENT_SCHEMA.getSchemaHash().toByteArray());
      // The schema is sorted by name, hence it is {body, header}.
      // Serialize the body
      encoder.writeBytes(event.getBody());
      // Serialize the headers
      encoder.writeInt(headers.size());
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        String value = entry.getValue();
        encoder.writeString(entry.getKey())
               .writeInt(value == null ? 1 : 0)
               .writeString(entry.getValue());
      }
      if (headers.size() > 0) {
        encoder.writeInt(0);
      }
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
      // Read the body
      ByteBuffer body = decoder.readBytes();
      // Read the header
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
      return new DefaultStreamEvent(headers.build(), body);

    } catch (IOException e) {
      // It should never happens, otherwise something very wrong.
      throw Throwables.propagate(e);
    }
  }
}
