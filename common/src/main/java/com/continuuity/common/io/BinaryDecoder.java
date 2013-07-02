package com.continuuity.common.io;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link Decoder} for binary-format data.
 */
public final class BinaryDecoder implements Decoder {

  private final InputStream input;

  public BinaryDecoder(InputStream input) {
    this.input = input;
  }

  @Override
  public Object readNull() throws IOException {
    // No-op
    return null;
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
      val ^= (long) (b & 0x7f) << shift;
      shift += 7;
      b = input.read();
    }
    val ^= (long) b << shift;
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
    return Double.longBitsToDouble(((long) high << 32) | (low & 0xffffffffL));
  }

  @Override
  public String readString() throws IOException {
    return new String(rawReadBytes(), Charsets.UTF_8);
  }

  @Override
  public ByteBuffer readBytes() throws IOException {
    return ByteBuffer.wrap(rawReadBytes());
  }

  @Override
  public void skipFloat() throws IOException {
    // Skip 4 bytes
    skipBytes(4L);
  }

  @Override
  public void skipDouble() throws IOException {
    // Skip 8 bytes
    skipBytes(8L);
  }

  @Override
  public void skipString() throws IOException {
    skipBytes();
  }

  @Override
  public void skipBytes() throws IOException {
    skipBytes(readInt());
  }

  private void skipBytes(long len) throws IOException {
    long skipped = input.skip(len);
    while (skipped != len) {
      skipped += input.skip(len - skipped);
    }
  }

  private byte[] rawReadBytes() throws IOException {
    int toRead = readInt();
    byte[] bytes = new byte[toRead];
    while (toRead > 0) {
      toRead -= input.read(bytes, bytes.length - toRead, toRead);
    }
    return bytes;
  }
}
