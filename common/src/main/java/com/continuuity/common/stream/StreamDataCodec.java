/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.stream;

import com.continuuity.api.stream.StreamData;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Utility class for encode/decode {@link StreamData}
 */
public final class StreamDataCodec {

  public static final Schema STREAM_DATA_SCHEMA;

  static {
    Schema schema;
    try {
      schema = new ReflectionSchemaGenerator().generate(StreamData.class);
    } catch (UnsupportedTypeException e) {
      schema = null;
    }
    STREAM_DATA_SCHEMA = schema;
  }


  /**
   * Encodes the given {@link StreamData} using the {@link Encoder}.
   *
   * @param data The data to encode
   * @param encoder The encoder
   * @throws IOException If there is any IO error during encoding.
   */
  public static void encode(StreamData data, Encoder encoder) throws IOException {
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
   * Decodes from the given {@link Decoder} to reconstruct a {@link StreamData}.
   *
   * @param decoder Decoder to read data from.
   * @return A new instance of {@link StreamData}.
   * @throws IOException If there is any IO error during decoding.
   */
  public static StreamData decode(Decoder decoder) throws IOException {
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
    return new DefaultStreamData(headers.build(), body);
  }

  /**
   * Skips an encoded {@link StreamData}.
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

  private StreamDataCodec() {
  }
}
