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

package io.cdap.cdap.common.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Implements {@link SeekableInputStream} with a {@link FileInputStream}.
 */
public final class FileSeekableInputStream extends SeekableInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(FileSeekableInputStream.class);

  private final FileChannel fileChannel;
  private long markPos;

  public FileSeekableInputStream(FileInputStream in) {
    super(in);
    this.fileChannel = in.getChannel();
    this.markPos = -1L;
  }

  @Override
  public void seek(long pos) throws IOException {
    fileChannel.position(pos);
  }

  @Override
  public long getPos() throws IOException {
    return fileChannel.position();
  }

  @Override
  public boolean seekToNewSource(long targetPos) {
    return false;
  }

  @Override
  public long size() throws IOException {
    return fileChannel.size();
  }

  @Override
  public synchronized void mark(int readlimit) {
    try {
      markPos = getPos();
    } catch (IOException e) {
      LOG.warn("Failed to get position from file input", e);
      markPos = -1L;
    }
  }

  @Override
  public synchronized void reset() throws IOException {
    if (markPos < 0) {
      throw new IOException("Resetting to invalid mark");
    }
    seek(markPos);
  }

  @Override
  public boolean markSupported() {
    return true;
  }
}
