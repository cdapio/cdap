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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * A LogWriter that writes to file.
 */
public class LogFileWriter implements LogWriter {

  static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

  LogConfiguration config;
  FileSystem fileSystem;
  private FSDataOutputStream out;

  @Override
  public void configure(LogConfiguration config) throws IOException {
    // remember configuration
    this.config = config;
    // open a file system
    fileSystem = config.getFileSystem();
    // make sure the base path exists in the file system
    createPath(config.getLogFilePath());
    // open the file to write to (create or append)
    openFileForWrite(config.getLogFilePath(), makeFileName(0));
  }

  @Override
  public void log(LogEvent event) throws IOException {
    synchronized (this) {
      // append message to current file
      persistMessage(formatMessage(event));
      // if necessary, rotate the log
      // - check size of current file, if exceeds:
      if (getCurrentFileSize() > config.getSizeThreshold()) {
        // roll the existing log files
        roll();
        // - open a new file
        openFileForWrite(config.getLogFilePath(), makeFileName(0));
      }
    }
  }

  private void roll() throws IOException {
    // close current
    closeFile();
    // delete oldest
    deleteFile(config.getLogFilePath(),
        makeFileName(config.getMaxInstances() - 1));
    // rename all files
    for (int i = config.getMaxInstances() - 1; i > 0; --i) {
      renameFile(config.getLogFilePath(),
          makeFileName(i - 1), makeFileName(i));
    }
  }

  @Override
  public void close() throws IOException {
    closeFile();
  }

  String formatMessage(LogEvent event) {
      return String.format("%s [%s] %s", event.getTag(),
          event.getLevel(), event.getMessage());
  }

  String makeFileName(int instance) {
    if (instance == 0) {
      return config.getLogFileName();
    } else {
      return String.format("%s.%d", config.getLogFileName(), instance);
    }
  }

  void createPath(String path) throws IOException {
    fileSystem.mkdirs(new Path(path));
  }

  void openFileForWrite(String path, String name) throws IOException {
    Path filePath = new Path(path, name);
    if (!fileSystem.exists(filePath)) {
      out = fileSystem.create(filePath);
    } else {
      out = fileSystem.append(filePath);
    }
  }

  void closeFile() throws IOException {
    synchronized (this) {
      if (out != null) {
        out.close();
        out = null;
      }
    }
  }

  void persistMessage(String message) throws IOException {
    synchronized (this) {
      out.write(message.getBytes(CHARSET_UTF8));
      out.write('\n');
      out.hflush();
    }
  }

  @Override
  public long getWritePosition() throws IOException {
    return getCurrentFileSize();
  }

  long getCurrentFileSize() throws IOException {
    return out.getPos();
  }

  void deleteFile(String path, String name) throws IOException {
    if (fileSystem.exists(new Path(path, name))) {
      fileSystem.delete(new Path(path, name), false);
    }
  }

  void renameFile(String path, String oldName, String newName)
      throws IOException {
    Path oldPath = new Path(path, oldName);
    if (!fileSystem.exists(oldPath)) {
      return;
    }
    Path newPath = new Path(path, newName);
    fileSystem.rename(oldPath, newPath);
  }
}
