/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.etl.api.validation;

import java.io.IOException;
import java.io.InputStream;

/**
 * A helper for implementing a {@link SeekableInputStream} where subclasses only need to implement
 * the methods not present in java's InputStream.
 */
public abstract class DelegatingSeekableInputStream extends SeekableInputStream {

  private final InputStream inputStream;

  public DelegatingSeekableInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return inputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return inputStream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return inputStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return inputStream.available();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    inputStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    inputStream.reset();
  }

  @Override
  public boolean markSupported() {
    return inputStream.markSupported();
  }

  @Override
  public int read() throws IOException {
    return inputStream.read();
  }
}
