package com.continuuity.api.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 *
 */
public final class BinaryDecoder implements Decoder {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final InputStream input;

  public BinaryDecoder(InputStream input) {
    this.input = input;
  }

  @Override
  public void readNull() throws IOException {
    // No-op
  }

  @Override
  public boolean readBool() throws IOException {
    return input.read() == 1;
  }

  @Override
  public int readInt() throws IOException {
    int val = 0;
    int shift = 0;
    int b = input.read();
    while (b > 0x7f) {
      val ^= (b & 0x7f) << shift;
      shift += 7;
      b = input.read();
    }
    val ^= b << shift;
    return (val >>> 1) ^ -(val & 1);
  }

  @Override
  public long readLong() throws IOException {
    long val = 0;
    int shift = 0;
    int b = input.read();
    while (b > 0x7f) {
      val ^= (long)(b & 0x7f) << shift;
      shift += 7;
      b = input.read();
    }
    val ^= (long)b << shift;
    return (val >>> 1) ^ -(val & 1);
  }

  @Override
  public float readFloat() throws IOException {
    int bits = input.read() ^ (input.read() << 8) ^ (input.read() << 16) ^ (input.read() << 24);
    return Float.intBitsToFloat(bits);
  }

  @Override
  public double readDouble() throws IOException {
    int low = input.read() ^ (input.read() << 8) ^ (input.read() << 16) ^ (input.read() << 24);
    int high = input.read() ^ (input.read() << 8) ^ (input.read() << 16) ^ (input.read() << 24);
    return Double.longBitsToDouble(((long)high << 32) | (low & 0xffffffffL));
  }

  @Override
  public String readString() throws IOException {
    return new String(rawReadBytes(), UTF8);
  }

  @Override
  public ByteBuffer readBytes() throws IOException {
    return ByteBuffer.wrap(rawReadBytes());
  }

  private byte[] rawReadBytes() throws IOException {
    int len = readInt();
    byte[] bytes = new byte[len];
    int readLen = 0;
    readLen = input.read(bytes);
    while (readLen != len) {
      readLen += input.read(bytes, readLen, len - readLen);
    }
    return bytes;
  }
}
