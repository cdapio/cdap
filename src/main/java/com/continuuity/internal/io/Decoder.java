package com.continuuity.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public interface Decoder {

  /**
   * Returns a null value.
   *
   * @return Always returns {@code null}
   * @throws IOException
   */
  Object readNull() throws IOException;

  boolean readBool() throws IOException;

  int readInt() throws IOException;

  long readLong() throws IOException;

  float readFloat() throws IOException;

  double readDouble() throws IOException;

  String readString() throws IOException;

  ByteBuffer readBytes() throws IOException;
}
