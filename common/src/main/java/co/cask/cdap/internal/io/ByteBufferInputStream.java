/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 *
 */
@NotThreadSafe
public final class ByteBufferInputStream extends InputStream {

  private ByteBuffer buffer;

  public ByteBufferInputStream(ByteBuffer buffer) {
    reset(buffer);
  }

  public ByteBufferInputStream reset(ByteBuffer buffer) {
    this.buffer = buffer;
    return this;
  }

  @Override
  public int read() throws IOException {
    if (buffer.remaining() <= 0) {
      return -1;
    }
    return buffer.get() & 0xff;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int remaining = buffer.remaining();
    if (remaining <= 0) {
      return -1;
    }
    if (len <= remaining) {
      buffer.get(b, off, len);
      return len;
    } else {
      buffer.get(b, off, remaining);
      return remaining;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n > Integer.MAX_VALUE) {
      throw new IOException("Cannot skip more then " + n + " bytes.");
    }
    int skips = (int) n;
    if (skips > buffer.remaining()) {
      skips = buffer.remaining();
    }
    buffer.position(buffer.position() + skips);
    return skips;
  }
}
