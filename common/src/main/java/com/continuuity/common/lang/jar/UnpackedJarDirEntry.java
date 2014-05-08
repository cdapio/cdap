package com.continuuity.common.lang.jar;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 *
 */
public class UnpackedJarDirEntry {

  private final File unpackedJarDir;
  private final File file;

  public UnpackedJarDirEntry(File unpackedJarDir, File file) {
    this.unpackedJarDir = unpackedJarDir;
    this.file = file;
  }

  public boolean isDirectory() {
    return file.isDirectory();
  }

  public long getSize() {
    return file.length();
  }

  public InputStream getInputStream() {
    try {
      return new FileInputStream(file);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  public File getFile() {
    return file;
  }

  public String getName() {
    return unpackedJarDir.toURI().relativize(file.toURI()).getPath();
  }
}
