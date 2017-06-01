/*
 * Copyright © 2017 Cask Data, Inc.
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

package org.apache.hadoop.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;

/** SFTP FileSystem input stream. */
class SFTPInputStream extends FSInputStream {

  public static final String E_SEEK_NOTSUPPORTED = "Seek not supported";
  public static final String E_CLIENT_NULL =
    "SFTP client null or not connected";
  public static final String E_NULL_INPUTSTREAM = "Null InputStream";
  public static final String E_STREAM_CLOSED = "Stream closed";
  public static final String E_CLIENT_NOTCONNECTED = "Client not connected";

  private InputStream wrappedStream;
  private ChannelSftp channel;
  private FileSystem.Statistics stats;
  private boolean closed;
  private long pos;

  SFTPInputStream(InputStream stream, ChannelSftp channel,
                  FileSystem.Statistics stats) {

    if (stream == null) {
      throw new IllegalArgumentException(E_NULL_INPUTSTREAM);
    }
    if (channel == null || !channel.isConnected()) {
      throw new IllegalArgumentException(E_CLIENT_NULL);
    }
    this.wrappedStream = stream;
    this.channel = channel;
    this.stats = stats;

    this.pos = 0;
    this.closed = false;
  }

  // We don't support seek unless the current position is same as the desired position.
  @Override
  public void seek(long position) throws IOException {
    // If seek is to the current pos, then simply return. This logic was added so that the seek call in
    // LineRecordReader#initialize method to '0' does not fail.
    if (getPos() == position) {
      return;
    }
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException(E_SEEK_NOTSUPPORTED);
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException(E_STREAM_CLOSED);
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null & byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  public synchronized int read(byte[] buf, int off, int len)
    throws IOException {
    if (closed) {
      throw new IOException(E_STREAM_CLOSED);
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null & result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    super.close();
    closed = true;
    if (!channel.isConnected()) {
      throw new IOException(E_CLIENT_NOTCONNECTED);
    }

    try {
      Session session = channel.getSession();
      channel.disconnect();
      session.disconnect();
    } catch (JSchException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }
}
