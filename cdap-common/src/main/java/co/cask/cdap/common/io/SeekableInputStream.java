/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import org.apache.hadoop.fs.Seekable;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract base class for {@link InputStream} that implements the {@link Seekable} interface.
 */
public abstract class SeekableInputStream extends FilterInputStream implements Seekable {

  private final long length;

  protected SeekableInputStream(InputStream in, long length) {
    super(in);
    this.length = length;
  }

  /**
   * Returns the seek position from the beginning of the file. If the given
   * position >= 0, the same value will be returned. Other, the value of (length + pos) will be returned.
   *
   * @throws IOException if the position given is beyond the end of stream
   */
  protected long getSeekPos(long pos) throws IOException {
    long position = pos >= 0 ? pos : length + pos;
    if (position < 0 || position > length) {
      throw new IOException("Invalid seek position. Size: " + length + ", seek position: " + position);
    }
    return position;
  }

  /**
   * Seek to the given position from the start of the file.
   * The next read() will be from that position.
   * If the given position is negative, it will
   * seek to (file_length + pos).
   *
   * @throws IOException if try to seek before beginning or after end of stream
   */
  @Override
  public abstract void seek(long pos) throws IOException;

  /**
   * Creates a {@link SeekableInputStream} from the given {@link InputStream}. Exception will be
   * thrown if failed to do so.
   *
   * @throws java.io.IOException If the given input stream is not seekable.
   */
  public static SeekableInputStream create(InputStream input, long length) throws IOException {
    if (input instanceof FileInputStream) {
      return create((FileInputStream) input, length);
    }
    if (input instanceof Seekable) {
      final Seekable seekable = (Seekable) input;
      return new SeekableInputStream(input, length) {
        @Override
        public void seek(long pos) throws IOException {
          seekable.seek(getSeekPos(pos));
        }

        @Override
        public long getPos() throws IOException {
          return seekable.getPos();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
          return seekable.seekToNewSource(getSeekPos(targetPos));
        }
      };
    }

    throw new IOException("Failed to create SeekableInputStream from " + input.getClass());
  }

  /**
   * Creates a {@link SeekableInputStream} from the given {@link FileInputStream}.
   */
  private static SeekableInputStream create(final FileInputStream input, long length) {
    return new SeekableInputStream(input, length) {
      @Override
      public void seek(long pos) throws IOException {
        input.getChannel().position(getSeekPos(pos));
      }

      @Override
      public long getPos() throws IOException {
        return input.getChannel().position();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
      }
    };
  }
}
