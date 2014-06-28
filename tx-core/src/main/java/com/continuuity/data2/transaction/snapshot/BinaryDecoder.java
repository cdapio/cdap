package com.continuuity.data2.transaction.snapshot;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * An decoder to help read snapshots in binary format.
 */
public final class BinaryDecoder {

  private final InputStream input;

  /**
   * @param input Stream to read from.
   */
  public BinaryDecoder(InputStream input) {
    this.input = input;
  }

  /**
   * Read one int from the input.
   * @return the read number
   * @throws java.io.IOException If there is IO error.
   * @throws java.io.EOFException If end of file reached.
   */
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

  /**
   * Read one long int from the input.
   * @return the read number
   * @throws java.io.IOException If there is IO error.
   * @throws java.io.EOFException If end of file reached.
   */
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

  /**
   * Read a byte sequence. First read an int to indicate how many bytes to read, then that many bytes.
   * @return the read bytes as a byte array
   * @throws java.io.IOException If there is IO error.
   * @throws java.io.EOFException If end of file reached.
   */
  public byte[] readBytes() throws IOException {
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
   * Reads a single byte value.
   *
   * @return The byte value read.
   * @throws java.io.IOException If there is IO error.
   * @throws java.io.EOFException If end of file reached.
   */
  private int readByte() throws IOException {
    int b = input.read();
    if (b == -1) {
      throw new EOFException();
    }
    return b;
  }
}
