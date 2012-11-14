package com.continuuity.common.logging;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  public List<String> tail(int size) throws IOException {
    return tail(new ArrayList<String>(), 0, size);
  }

  private List<String> tail(ArrayList<String> lines, int i, long size)
      throws IOException {

    // get the path of the current log file instance (xxx.log[.i])
    Path path = new Path(config.getLogFilePath(), makeFileName(i));

    // check for its existence, if it does not exist, return empty list
    if (!fileSystem.exists(path)) return lines;
    FileStatus status = fileSystem.getFileStatus(path);
    if (!status.isFile()) return lines;

    long fileSize = status.getLen();
    long seekPos = 0;
    long bytesToRead = size;
    if (fileSize >= size) {
      // if size of currentFile is sufficient, we need to seek to the
      // position that is size bytes from the end of the file.
      seekPos = fileSize - size;
    } else {
      // if size of current file is less than limit, make a recursive
      // call to tail for previous file
      tail(lines, i + 1, size - fileSize);
      bytesToRead = fileSize;
    }

    // open current file for reading
    FSDataInputStream input = fileSystem.open(path);
    // seek into latest file
    if (seekPos > 0) {
      input.seek(seekPos);
    }

    // read to the end of current file
    byte[] bytes = new byte[(int)bytesToRead];
    input.readFully(bytes);

    int pos = 0;
    if (seekPos > 0) {
      // if we seeked into the file, then we are likely in the middle of the
      // line, and we want to skip up to the first new line
      while (pos < bytesToRead && bytes[pos] != '\n') pos++;
      pos++; // now we are just after the first new line
    }

    // read lines until the end of the buffer
    while (pos < bytesToRead) {
      int start = pos;
      while (pos < bytesToRead && bytes[pos] != '\n') pos++;
      // now we are at end of file or at the new line
      if (pos != start) { // ignore empty lines
        String line = new String(bytes, start, pos - start,
            LogFileWriter.charsetUtf8);
        lines.add(line);
      }
      pos++; // skip the new line character
    }
    return lines;
  }

  String makeFileName(int instance) {
    if (instance == 0) {
      return config.getLogFileName();
    } else {
      return String.format("%s.%d", config.getLogFileName(), instance);
    }
  }

}
