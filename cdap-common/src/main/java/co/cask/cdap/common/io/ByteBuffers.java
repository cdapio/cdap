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

package co.cask.cdap.common.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A utility class that helps to use a {@link ByteBuffer} correctly and efficiently.
 */
public final class ByteBuffers {

  public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private static final int MAX_BUFFER_SIZE = 64 * 1024;

  /**
   * Writes all remaining bytes of a {@link ByteBuffer} to an {@link OutputStream}. The position and limit of
   * the buffer is not modified after this call returns.
   */
  public static void writeToStream(ByteBuffer buffer, OutputStream outputStream) throws IOException {
    if (buffer.hasArray()) {
      outputStream.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      return;
    }

    // If the buffer is not backed by an array, need to copy the buffer content into an array first
    buffer.mark();
    byte[] bytes = new byte[Math.min(buffer.remaining(), MAX_BUFFER_SIZE)];
    while (buffer.hasRemaining()) {
      int len = Math.min(buffer.remaining(), bytes.length);
      buffer.get(bytes, 0, len);
      outputStream.write(bytes, 0, len);
    }
    buffer.reset();
  }

  private ByteBuffers() {
  }
}
