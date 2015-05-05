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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Implements {@link SeekableInputStream} with a {@link FileInputStream}.
 */
final class FileSeekableInputStream extends SeekableInputStream {

  private final FileChannel fileChannel;

  FileSeekableInputStream(FileInputStream in) {
    super(in);
    this.fileChannel = in.getChannel();
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
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  @Override
  public long size() throws IOException {
    return fileChannel.size();
  }
}
