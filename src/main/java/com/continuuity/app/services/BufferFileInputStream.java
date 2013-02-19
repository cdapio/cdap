package com.continuuity.app.services;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

/**
 * File input stream that returns byte array while reading.
 */
public final class BufferFileInputStream {

  /**
   * Buffer for holding bytes read from file.
   */
  private final byte[] buffer;

  /**
   * Inputs stream for the file to be read.
   */
  private final FileInputStream stream;

  /**
   * Constructor of BufferFileInputStream with defined buffer size.
   *
   * @param file to read
   * @param bufferSize of the bytes.
   * @throws java.io.FileNotFoundException
   */
  public BufferFileInputStream(String file, int bufferSize) throws FileNotFoundException {
    stream = new FileInputStream(file);
    buffer = new byte[bufferSize];
  }

  /**
   * Constructor of BufferFileInputStream with default buffer stream size.
   *
   * @param file The File to read
   * @throws java.io.FileNotFoundException
   */
  public BufferFileInputStream(String file) throws FileNotFoundException {
    this(file, 100*1024);
  }

  /**
   * Reads block of byte size from the stream and returns a byte array.
   *
   * @return byte array of bytes read from stream.
   * @throws java.io.IOException
   */
  public byte[] read() throws IOException {
    int length = stream.read(buffer);
    if(length==-1) {
      close();
      return new byte[0];
    } else if(length==buffer.length) {
      return buffer;
    } else {
      return Arrays.copyOf(buffer, length);
    }
  }

  /**
   * Closes the stream.
   *
   * @throws java.io.IOException
   */
  public void close() throws IOException {
    stream.close();
  }
}