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
