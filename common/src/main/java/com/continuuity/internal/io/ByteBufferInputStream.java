/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

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
