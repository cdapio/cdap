package com.continuuity.common.lang.jar;


import java.io.File;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Like JarInputStream, but for an unpacked jar directory instead of a jar file.
 */
public class UnpackedJarDirIterator implements Enumeration<UnpackedJarDirEntry> {

  private final File bundleJarDir;
  private final Queue<File> fileQueue = new LinkedList<File>();

  private File nextFile;

  public UnpackedJarDirIterator(File bundleJarDir) {
    this.bundleJarDir = bundleJarDir;
    fileQueue.add(bundleJarDir);
    nextFile = seekToNextFile();
  }

  @Override
  public boolean hasMoreElements() {
    return nextFile != null;
  }

  @Override
  public UnpackedJarDirEntry nextElement() {
    File result = nextFile;
    nextFile = seekToNextFile();
    return new UnpackedJarDirEntry(bundleJarDir, result);
  }

  public void close() {
    fileQueue.clear();
  }

  private File seekToNextFile() {
    File currFile;

    do {
      currFile = fileQueue.poll();
      if (currFile != null && currFile.isDirectory()) {
        File[] children = currFile.listFiles();
        if (children != null) {
          for (File file : children) {
            fileQueue.add(file);
          }
        }
      }
    } while (!fileQueue.isEmpty() && currFile != null && currFile.isDirectory());

    return currFile;
  }

}
