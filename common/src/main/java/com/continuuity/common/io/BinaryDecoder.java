package com.continuuity.common.io;

import com.google.common.base.Charsets;

import java.io.EOFException;
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
    return readByte() == 1;
  }

  @Override
  public int readInt() throws IOException {
    int val = 0;
    int shift = 0;
    int b = readByte();
    while (b > 0x7f) {
      val ^= (b & 0x7f) << shift;
      shift += 7;
      b = readByte();
    }
    val ^= b << shift;
    return (val >>> 1) ^ -(val & 1);
  }

  @Override
  public long readLong() throws IOException {
    long val = 0;
    int shift = 0;
    int b = readByte();
    while (b > 0x7f) {
      val ^= (long) (b & 0x7f) << shift;
      shift += 7;
      b = readByte();
    }
    val ^= (long) b << shift;
    return (val >>> 1) ^ -(val & 1);
  }

  @Override
  public float readFloat() throws IOException {
    int bits = readByte() ^ (readByte() << 8) ^ (readByte() << 16) ^ (readByte() << 24);
    return Float.intBitsToFloat(bits);
  }

  @Override
  public double readDouble() throws IOException {
    int low = readByte() ^ (readByte() << 8) ^ (readByte() << 16) ^ (readByte() << 24);
    int high = readByte() ^ (readByte() << 8) ^ (readByte() << 16) ^ (readByte() << 24);
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
    long skipped = 0;
    while (skipped != len) {
      long skip = input.skip(len - skipped);
      if (skip == 0) {
        throw new EOFException();
      }
      skipped += skip;
    }
  }

  private byte[] rawReadBytes() throws IOException {
    int toRead = readInt();
    byte[] bytes = new byte[toRead];
    while (toRead > 0) {
      int byteRead = input.read(bytes, bytes.length - toRead, toRead);
      if (byteRead == -1) {
        throw new EOFException();
      }
      toRead -= byteRead;
    }
    return bytes;
  }

  /**
   * Reads a byte value.
   *
   * @return The byte value read.
   * @throws IOException If there is IO error.
   * @throws EOFException If end of file reached.
   */
  private int readByte() throws IOException {
    int b = input.read();
    if (b == -1) {
      throw new EOFException();
    }
    return b;
  }
}
