package com.continuuity.internal.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
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

  Encoder writeBytes(ByteBuffer bytes) throws IOException;
}
