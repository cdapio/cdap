/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
  private final InputStream stream;

  /**
   * Constructor of BufferFileInputStream with defined buffer size.
   *
   * @param file to read
   * @param bufferSize of the bytes.
   * @throws java.io.FileNotFoundException
   */
  public BufferFileInputStream(String file, int bufferSize) throws FileNotFoundException {
    this(new FileInputStream(file), bufferSize);
  }

  /**
   * Constructor of BufferFileInputStream with default buffer stream size.
   *
   * @param file The File to read
   * @throws java.io.FileNotFoundException
   */
  public BufferFileInputStream(String file) throws FileNotFoundException {
    this(file, 100 * 1024);
  }

  /**
   * Constructor of BufferFileInputStream using an input stream.
   *
   * @param stream to be read from
   * @param size of the buffer.
   */
  public BufferFileInputStream(InputStream stream, int size) {
    this.stream = stream;
    this.buffer = new byte[size];
  }

  /**
   * Reads block of byte size from the stream and returns a byte array.
   *
   * @return byte array of bytes read from stream.
   * @throws java.io.IOException
   */
  public byte[] read() throws IOException {
    int length = stream.read(buffer);
    if (length == -1) {
      close();
      return new byte[0];
    } else if (length == buffer.length) {
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
