/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.common.logging;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A LogReader reading from file.
 */
public class LogFileReader implements LogReader {
  LogConfiguration config;
  FileSystem fileSystem;

  @Override
  public void configure(LogConfiguration config) throws IOException {
    // remember configuration
    this.config = config;
    // open a file system
    fileSystem = config.getFileSystem();
  }

  @Override
  public List<String> tail(int sizeToRead, long writePos) throws IOException {
    return tail(new ArrayList<String>(), 0, sizeToRead, writePos);
  }


  /**
   * Recursive method to tail the log. Reads from the current log file
   * instance (i), and if that does not have sufficient size, recurses to the
   * next older instance (i+1). If the caller knows the size of the current
   * file (i), he can pass it via the fileSize parameter.
   * @param lines A list of log lines to append read lines to
   * @param i The current log file instance to start reading from
   * @param size number of bytes to read at most
   * @param sizeHint if known, the caller should pass in the length of the
   *                 current log file instance. This helps to seek to the end
   *                 of a file that has not been closed yet (and hence file
   *                 status does not reflect its correct size). Only needed
   *                 at instance 0. Otherwise (for recursive calls) this is
   *                 -1, and the file size will be obatained via file status.
   * @return The list of lines read
   * @throws IOException if reading goes badly wrong
   */
  private List<String> tail(ArrayList<String> lines, int i, long size,
                            long sizeHint)
      throws IOException {

    // get the path of the current log file instance (xxx.log[.i])
    Path path = new Path(config.getLogFilePath(), makeFileName(i));

    // check for its existence, if it does not exist, return empty list
    if (!fileSystem.exists(path)) {
      return lines;
    }
    FileStatus status = fileSystem.getFileStatus(path);
    if (!status.isFile()) {
      return lines;
    }

    long fileSize;
    if (sizeHint >= 0) {
      fileSize = sizeHint;
    } else if (i > 0) {
      fileSize = status.getLen();
    } else {
      fileSize = determineTrueFileSize(path, status);
    }

    long seekPos = 0;
    long bytesToRead = size;
    if (fileSize >= size) {
      // if size of currentFile is sufficient, we need to seek to the
      // position that is size bytes from the end of the file.
      seekPos = fileSize - size;
    } else {
      // if size of current file is less than limit, make a recursive
      // call to tail for previous file
      tail(lines, i + 1, size - fileSize, -1);
      bytesToRead = fileSize;
    }

    // open current file for reading
    byte[] bytes = new byte[(int) bytesToRead];
    FSDataInputStream input = fileSystem.open(path);
    try {
      // seek into latest file
      if (seekPos > 0) {
        input.seek(seekPos);
      }
      // read to the end of current file
      input.readFully(bytes);
    } finally {
      input.close();
    }
    int pos = 0;
    if (seekPos > 0) {
      // if we seeked into the file, then we are likely in the middle of the
      // line, and we want to skip up to the first new line
      while (pos < bytesToRead && bytes[pos] != '\n') {
        pos++;
      }
      pos++; // now we are just after the first new line
    }

    // read lines until the end of the buffer
    while (pos < bytesToRead) {
      int start = pos;
      while (pos < bytesToRead && bytes[pos] != '\n') {
        pos++;
      }
      // now we are at end of file or at the new line
      if (pos != start) { // ignore empty lines
        String line = new String(bytes, start, pos - start,
            LogFileWriter.CHARSET_UTF8);
        lines.add(line);
      }
      pos++; // skip the new line character
    }
    return lines;
  }

  private long determineTrueFileSize(Path path, FileStatus status)
      throws IOException {
    FSDataInputStream stream = fileSystem.open(path);
    try {
      stream.seek(status.getLen());
      // we need to read repeatedly until we reach the end of the file
      byte[] buffer = new byte[1024 * 1024];
      while (stream.read(buffer, 0, buffer.length) >= 0) {
        // empty body.
      }
      long trueSize = stream.getPos();
      return trueSize;
    } finally {
      stream.close();
    }
  }

  String makeFileName(int instance) {
    if (instance == 0) {
      return config.getLogFileName();
    } else {
      return String.format("%s.%d", config.getLogFileName(), instance);
    }
  }

}
