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

package co.cask.cdap.data.stream;

import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.common.io.Decoder;
import co.cask.cdap.common.io.SeekableInputStream;
import co.cask.cdap.common.stream.StreamEventDataCodec;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.common.io.ByteBufferInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A buffer for holding encoded stream events. It is used by {@link StreamDataFileReader} for holding
 * encoded stream events in each data block.
 */
@NotThreadSafe
final class StreamEventBuffer {

  private final ByteBufferInputStream bufferInput;
  private final Decoder decoder;

  private ByteBuffer buffer;
  private long basePosition;

  StreamEventBuffer() {
    this.buffer = ByteBuffers.EMPTY_BUFFER;
    this.bufferInput = new ByteBufferInputStream(buffer);
    this.decoder = new BinaryDecoder(bufferInput);
    this.basePosition = -1L;
  }

  /**
   * Fills the internal buffer by reading from the given input stream.
   *
   * @param input input stream to read from
   * @param size number of bytes to read
   * @throws IOException if failed to read from the stream
   * @throws EOFException if failed to read the given number of bytes from the input
   */
  void fillBuffer(SeekableInputStream input, int size) throws IOException {
    buffer.clear();
    buffer = ensureCapacity(buffer, size);

    try {
      basePosition = input.getPos();
      int bytesRead = 0;
      while (bytesRead != size) {
        int len = input.read(buffer.array(), bytesRead, size - bytesRead);
        if (len < 0) {
          throw new EOFException("Expected to read " + size + ", but only " + bytesRead + " was read");
        }
        bytesRead += len;
      }
      buffer.limit(size);
      bufferInput.reset(buffer);
    } catch (IOException e) {
      // Make the buffer has nothing to read
      buffer.position(buffer.limit());
      basePosition = -1L;
      throw e;
    }
  }

  /**
   * Returns {@code true} if there are events in the buffer, {@code false} otherwise.
   */
  boolean hasEvent() {
    return buffer.hasRemaining();
  }

  /**
   * Returns the position in the stream that this buffer is currently at or {@code -1} if nothing has been
   * read from the stream.
   */
  long getPosition() {
    return basePosition >= 0 ? basePosition + buffer.position() : -1L;
  }

  /**
   * Returns the position in the stream that represents the end of this buffer or {@code -1} if nothing has
   * been read from the stream.
   */
  long getEndPosition() {
    return basePosition >= 0 ? basePosition + buffer.limit() : -1L;
  }

  /**
   * Decodes a stream event from the buffer.
   *
   * @param timestamp timestamp of the {@link PositionStreamEvent} created
   * @param filter filter to apply to decide reading or skipping event
   * @return A {@link PositionStreamEvent} if the filter accept it, or {@code null} if rejected by the filter
   * @throws IOException if fails to decode event from the buffer
   */
  PositionStreamEvent nextEvent(long timestamp, ReadFilter filter) throws IOException {
    if (!hasEvent()) {
      throw new IOException("No more event in the buffer");
    }

    long eventPos = basePosition + buffer.position();
    if (filter.acceptOffset(eventPos)) {
      return new PositionStreamEvent(StreamEventDataCodec.decode(decoder), timestamp, eventPos);
    }
    StreamEventDataCodec.skip(decoder);
    return null;
  }

  /**
   * Ensures that the given {@link ByteBuffer} is of sufficient size.
   *
   * @param buffer The buffer to check for capacity
   * @param size Capacity needed
   * @return The given buffer if it is of sufficient size; otherwise, a new buffer of the given size will be returned.
   */
  private ByteBuffer ensureCapacity(ByteBuffer buffer, int size) {
    return (buffer.remaining() >= size) ? buffer : ByteBuffer.allocate(size);
  }
}
