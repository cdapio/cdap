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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of {@link SeekableInputStream} for {@link Location}.
 */
final class DFSSeekableInputStream extends SeekableInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(DFSSeekableInputStream.class);

  private final Seekable seekable;
  private final StreamSizeProvider sizeProvider;

  /**
   * Creates an instance with the given {@link FSDataInputStream}.
   *
   * @param input the stream for the actual IO operations
   * @param sizeProvider a {@link StreamSizeProvider} to get stream size
   */
  DFSSeekableInputStream(FSDataInputStream input, StreamSizeProvider sizeProvider) {
    super(input);
    this.seekable = input;
    this.sizeProvider = sizeProvider;
  }

  @Override
  public long size() throws IOException {
    return sizeProvider.size();
  }

  @Override
  public void seek(long pos) throws IOException {
    seekable.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return seekable.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return seekable.seekToNewSource(targetPos);
  }
}
