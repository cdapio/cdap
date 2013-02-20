package com.continuuity.common.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for encoding data.
 */
public interface Encoder {

  Encoder writeNull() throws IOException;

  Encoder writeBool(boolean b) throws IOException;

  Encoder writeInt(int i) throws IOException;

  Encoder writeLong(long l) throws IOException;

  Encoder writeFloat(float f) throws IOException;

  Encoder writeDouble(double d) throws IOException;

  Encoder writeString(String s) throws IOException;

  Encoder writeBytes(byte[] bytes) throws IOException;

  Encoder writeBytes(byte[] bytes, int off, int len) throws IOException;

  /**
   * Writes out the remaining bytes in {@link ByteBuffer}.
   * The given {@link ByteBuffer} is untounch after this method is returned (i.e. same position and limit).
   *
   * @param bytes
   * @return
   * @throws IOException
   */
  Encoder writeBytes(ByteBuffer bytes) throws IOException;
}
