package com.continuuity.common.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for decoding data.
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

  /**
   * Skips a float.
   */
  void skipFloat() throws IOException;

  /**
   * Skips a double.
   */
  void skipDouble() throws IOException;

  /**
   * Skips the a string.
   */
  void skipString() throws IOException;

  /**
   * Skips a byte array
   */
  void skipBytes() throws IOException;
}
