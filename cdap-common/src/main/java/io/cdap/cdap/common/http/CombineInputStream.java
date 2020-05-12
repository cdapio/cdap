/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import com.google.common.io.Closeables;
import io.cdap.cdap.common.io.FileSeekableInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import javax.annotation.Nullable;

/**
 * An {@link InputStream} that reads from an in-memory {@link ByteBuf} and optionally a file.
 * It is used by the {@link SpillableBodyConsumer} to combine in-memory data with spilled data.
 */
final class CombineInputStream extends InputStream {

  private final InputStream bufferStream;
  private final InputStream spillStream;

  CombineInputStream(ByteBuf buffer, @Nullable Path spillPath) throws IOException {
    this.bufferStream = new ByteBufInputStream(buffer);
    this.spillStream = spillPath == null ? null : new FileSeekableInputStream(new FileInputStream(spillPath.toFile()));
  }

  @Override
  public int read() throws IOException {
    int b = bufferStream.read();
    if (b >= 0 || spillStream == null) {
      return b;
    }
    return spillStream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = bufferStream.read(b, off, len);
    if (spillStream == null || bytesRead == len) {
      return bytesRead;
    }
    if (bytesRead < 0) {
      bytesRead = 0;
    }

    return bytesRead + spillStream.read(b, off + bytesRead, len - bytesRead);
  }

  @Override
  public long skip(long n) throws IOException {
    long skipped = 0;
    if (bufferStream.available() > 0) {
      skipped = bufferStream.skip(n);
    }
    if (spillStream == null) {
      return skipped;
    }
    return skipped + spillStream.skip(n - skipped);
  }

  @Override
  public int available() throws IOException {
    return bufferStream.available() + (spillStream == null ? 0 : spillStream.available());
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(bufferStream);
    if (spillStream != null) {
      spillStream.close();
    }
  }

  @Override
  public synchronized void mark(int readlimit) {
    if (!markSupported()) {
      return;
    }
    bufferStream.mark(readlimit);
    if (spillStream != null) {
      spillStream.mark(readlimit);
    }
  }

  @Override
  public synchronized void reset() throws IOException {
    if (!markSupported()) {
      return;
    }
    bufferStream.reset();
    if (spillStream != null) {
      spillStream.reset();
    }
  }

  @Override
  public boolean markSupported() {
    return bufferStream.markSupported() && (spillStream == null || spillStream.markSupported());
  }
}
