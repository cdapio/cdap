package com.continuuity.internal.io;

import com.continuuity.common.io.Encoder;

import java.io.IOException;

/**
 * Represents writer for encoding object.
 *
 * @param <T> type T to serialized.
 */
public interface DatumWriter<T> {

  void encode(T data, Encoder encoder) throws IOException;
}
