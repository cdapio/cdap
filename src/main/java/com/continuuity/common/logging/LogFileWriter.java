package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LogFileWriter implements LogWriter {

  LogConfiguration config;
  FileSystem fileSystem;
  private FSDataOutputStream out;

  @Override
  public void configure(LogConfiguration config) throws IOException {
    // remember configuration
    this.config = config;
    // open a file system
    fileSystem = FileSystem.get(CConfiguration.create());
    // make sure the base path exists in the file system
    createPath(config.getLogFilePath());
    // open the file to write to (create or append)
    openFileForWrite(config.getLogFilePath(), makeFileName(0));
  }

  @Override
  public void log(LogEvent event) throws IOException {
    synchronized(this) {
      // append message to current file
      persistMessage(formatMessage(event));
      // if necessary, rotate the log
      // - check size of current file, if exceeds:
      if (getCurrentFileSize() > config.getSizeThreshold()) {
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
        // - open a new file
        openFileForWrite(config.getLogFilePath(), makeFileName(0));
      }
    }
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
    Path fsPath = new Path(path, name);
    if (!fileSystem.exists(fsPath)) {
      this.fileSystem.create(fsPath);
    }
    out = fileSystem.append(fsPath);
  }

  void closeFile() throws IOException {
    out.close();
  }

  void persistMessage(String message) throws IOException {
    out.writeUTF(message);
  }

  long getCurrentFileSize() throws IOException {
    return out.getPos();
  }

  void deleteFile(String path, String name) throws IOException {
    fileSystem.delete(new Path(path), false);
  }

  void renameFile(String path, String oldName, String newName)
      throws IOException {
    fileSystem.rename(new Path(path, oldName), new Path(path, newName));
  }
}
