/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service.upload;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * An {@link InputStream} implementation that reads data from {@link ByteBuf}.
 * It allows asynchronous update to add new buffer from this class to read from. The end of file
 * is signaled by appending an empty {@link ByteBuf} to this stream.
 */
public final class AsyncChannelBufferInputStream extends InputStream {

  private final BlockingQueue<ByteBuf> buffers = new SynchronousQueue<>();
  private ByteBuf currentBuffer = Unpooled.EMPTY_BUFFER;
  private boolean eof;

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    ByteBuf buffer = getCurrentBuffer();
    if (eof) {
      return -1;
    }
    int size = Math.min(len, buffer.readableBytes());
    buffer.readBytes(b, off, size);
    return size;
  }

  @Override
  public int read() throws IOException {
    ByteBuf buffer = getCurrentBuffer();
    return eof ? -1 : buffer.readByte();
  }

  @Override
  public void close() throws IOException {
    eof = true;

    // Need to poll from queue to unblock any thread that is trying to append
    ByteBuf buf;
    while ((buf = buffers.poll()) != null) {
      // release all buffers
      buf.release();
    }
    // Offer an empty to unblock the reader thread
    buffers.offer(Unpooled.EMPTY_BUFFER);
  }

  private ByteBuf getCurrentBuffer() throws IOException {
    try {
      if (!eof && !currentBuffer.isReadable()) {
        // Release the buffer when it is done reading
        currentBuffer.release();
        currentBuffer = buffers.take();
      }
      if (!currentBuffer.isReadable()) {
        currentBuffer.release();
        eof = true;
      }
      return currentBuffer;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Appends more content to be consumable through this stream. This method will block until
   * the append is completed.
   *
   * @throws InterruptedException if the append operation is interrupted
   */
  public void append(ByteBuf buffer) throws InterruptedException, IOException {
    if (eof) {
      throw new IOException("Stream already closed");
    }
    // Retain the buffer for processing asynchronously
    buffers.put(buffer.retain());
  }
}
