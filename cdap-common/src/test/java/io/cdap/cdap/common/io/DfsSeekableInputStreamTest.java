/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DFSSeekableInputStream}.
 */
public class DfsSeekableInputStreamTest {

  @Test
  public void testSeekReadSizeAndNormalClose() throws IOException {
    byte[] payload = "0123456789".getBytes(StandardCharsets.UTF_8);
    AtomicBoolean inputClosed = new AtomicBoolean(false);
    AtomicBoolean providerClosed = new AtomicBoolean(false);

    FSDataInputStream fsInput = new FSDataInputStream(
        new InMemoryFsInputStream(payload, inputClosed, false));
    CloseableSizeProvider sizeProvider = new CloseableSizeProvider(payload.length,
        providerClosed, false);

    try (DFSSeekableInputStream stream = new DFSSeekableInputStream(fsInput, sizeProvider)) {
      Assert.assertEquals(payload.length, stream.size());
      Assert.assertEquals(0L, stream.getPos());
      stream.seek(4L);
      Assert.assertEquals(4L, stream.getPos());
      Assert.assertEquals('4', (char) stream.read());
      Assert.assertFalse(stream.seekToNewSource(2L));
    }

    Assert.assertTrue(inputClosed.get());
    Assert.assertTrue(providerClosed.get());
  }

  @Test
  public void testCloseQuietlySwallowsSizeProviderCloseException() throws IOException {
    byte[] payload = "abc".getBytes(StandardCharsets.UTF_8);
    AtomicBoolean inputClosed = new AtomicBoolean(false);
    AtomicBoolean providerClosed = new AtomicBoolean(false);

    // 1. When super.close() succeeds and sizeProvider.close() throws IOException, close() succeeds.
    FSDataInputStream fsInput = new FSDataInputStream(
        new InMemoryFsInputStream(payload, inputClosed, false));
    DFSSeekableInputStream stream = new DFSSeekableInputStream(fsInput,
        new CloseableSizeProvider(3L, providerClosed, true));
    stream.close();
    Assert.assertTrue(inputClosed.get());
    Assert.assertTrue(providerClosed.get());

    // 2. When super.close() throws IOException and sizeProvider.close() also throws IOException,
    // sizeProvider.close() is still invoked and the original super.close() IOException propagates.
    AtomicBoolean bothInputClosed = new AtomicBoolean(false);
    AtomicBoolean bothProviderClosed = new AtomicBoolean(false);
    FSDataInputStream failingFsInput = new FSDataInputStream(
        new InMemoryFsInputStream(payload, bothInputClosed, true));
    DFSSeekableInputStream failingStream = new DFSSeekableInputStream(failingFsInput,
        new CloseableSizeProvider(3L, bothProviderClosed, true));
    try {
      failingStream.close();
      Assert.fail("Expected IOException from underlying input stream close");
    } catch (IOException e) {
      Assert.assertEquals("input close error", e.getMessage());
    }
    Assert.assertTrue(bothInputClosed.get());
    Assert.assertTrue(bothProviderClosed.get());

    // 3. Non-Closeable StreamSizeProvider works cleanly.
    FSDataInputStream plainInput = new FSDataInputStream(
        new InMemoryFsInputStream(payload, new AtomicBoolean(false), false));
    DFSSeekableInputStream plainStream = new DFSSeekableInputStream(plainInput, () -> 3L);
    Assert.assertEquals(3L, plainStream.size());
    plainStream.close();
  }

  private static final class CloseableSizeProvider implements StreamSizeProvider, Closeable {
    private final long size;
    private final AtomicBoolean closed;
    private final boolean failOnClose;

    CloseableSizeProvider(long size, AtomicBoolean closed, boolean failOnClose) {
      this.size = size;
      this.closed = closed;
      this.failOnClose = failOnClose;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      if (failOnClose) {
        throw new IOException("provider close error");
      }
    }
  }

  private static final class InMemoryFsInputStream extends FSInputStream {
    private final byte[] bytes;
    private final AtomicBoolean closed;
    private final boolean failOnClose;
    private int pos;

    InMemoryFsInputStream(byte[] bytes, AtomicBoolean closed, boolean failOnClose) {
      this.bytes = bytes;
      this.closed = closed;
      this.failOnClose = failOnClose;
    }

    @Override
    public void seek(long newPos) {
      this.pos = (int) newPos;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read() {
      return pos < bytes.length ? (bytes[pos++] & 0xFF) : -1;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      if (failOnClose) {
        throw new IOException("input close error");
      }
    }
  }
}
