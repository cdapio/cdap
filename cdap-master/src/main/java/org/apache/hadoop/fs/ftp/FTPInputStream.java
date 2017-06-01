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

package org.apache.hadoop.fs.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link FTPInputStream}, copied from Hadoop and modified, that doesn't throw an exception when seeks are attempted
 * to the current position. Position equality check logic in {@link FTPInputStream#seek} is the only change from the
 * original class in Hadoop. This change is required since {@link LineRecordReader} calls {@link FTPInputStream#seek}
 * with value of 0. TODO: This file can be removed once https://issues.cask.co/browse/CDAP-5387 is addressed.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FTPInputStream extends FSInputStream {

  InputStream wrappedStream;
  FTPClient client;
  FileSystem.Statistics stats;
  boolean closed;
  long pos;

  public FTPInputStream(InputStream stream, FTPClient client,
                        FileSystem.Statistics stats) {
    if (stream == null) {
      throw new IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  // We don't support seek unless the current position is same as the desired position.
  @Override
  public void seek(long pos) throws IOException {
    // If seek is to the current pos, then simply return. This logic was added so that the seek call in
    // LineRecordReader#initialize method to '0' does not fail.
    if (getPos() == pos) {
      return;
    }
    throw new IOException("Seek not supported");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Seek not supported");
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null && byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null && result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    super.close();
    closed = true;
    if (!client.isConnected()) {
      throw new FTPException("Client not connected");
    }

    boolean cmdCompleted = client.completePendingCommand();
    client.logout();
    client.disconnect();
    if (!cmdCompleted) {
      throw new FTPException("Could not complete transfer, Reply Code - "
                               + client.getReplyCode());
    }
  }

  // Not supported.

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }
}
