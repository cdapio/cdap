/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Deque;

/**
 * Copied from Google Guava as these methods are now deprecated.
 */
public final class DirUtils {

  private static final int TEMP_DIR_ATTEMPTS = 10000;

  /**
   * Utility classes should have a public constructor or a default constructor
   * hence made it private.
   */
  private DirUtils(){}

  /**
   * Wipes out all the a directory starting from a given directory.
   * @param directory to be cleaned
   * @throws IOException
   */
  public static void deleteDirectoryContents(File directory) throws IOException {
    Preconditions.checkArgument(directory.isDirectory(), "Not a directory: %s", directory);
    Deque<File> stack = Queues.newArrayDeque();
    stack.add(directory);

    while (!stack.isEmpty()) {
      File file = stack.peekLast();
      File[] files = file.listFiles();
      if (files == null || files.length == 0) {
        file.delete();
        stack.pollLast();
      } else {
        Collections.addAll(stack, files);
      }
    }
  }

  /**
   * Creates a temp directory inside the given base directory.
   *
   * @return the newly-created directory
   * @throws IllegalStateException if the directory could not be created
   */
  public static File createTempDir(File baseDir) {
    String baseName = System.currentTimeMillis() + "-";

    for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
      File tempDir = new File(baseDir, baseName + counter);
      if (tempDir.mkdirs()) {
        return tempDir;
      }
    }
    throw new IllegalStateException("Failed to create directory within "
                                      + TEMP_DIR_ATTEMPTS + " attempts (tried "
                                      + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
  }

  /**
   * Creates a directory if the directory doesn't exists.
   *
   * @param dir The directory to create
   * @return {@code true} if the directory exists or successfully created the directory.
   */
  public static boolean mkdirs(File dir) {
    // The last clause is needed so that if there are multiple threads trying to create the same directory
    // this method will still return true.
    return dir.isDirectory() || dir.mkdirs() || dir.isDirectory();
  }
}
