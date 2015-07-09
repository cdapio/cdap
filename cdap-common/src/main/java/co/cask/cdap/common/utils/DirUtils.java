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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Provides utility methods for operating on directories.
 */
public final class DirUtils {

  private static final int TEMP_DIR_ATTEMPTS = 10000;

  /**
   * Utility classes should have a public constructor or a default constructor
   * hence made it private.
   */
  private DirUtils(){}

  /**
   * Same as calling {@link #deleteDirectoryContents(File, boolean) deleteDirectoryContents(directory, false)}.
   */
  public static void deleteDirectoryContents(File directory) throws IOException {
    deleteDirectoryContents(directory, false);
  }

  /**
   * Wipes out content of a directory starting from a given directory.
   *
   * @param directory to be cleaned
   * @param retain if true, the given directory will be retained.
   * @throws IOException
   */
  public static void deleteDirectoryContents(File directory, boolean retain) throws IOException {
    if (!directory.isDirectory()) {
      throw new IOException("Not a directory: " + directory);
    }

    // avoid using guava's Queues.newArrayDeque() since this is a utility class that can be used in all sorts of
    // contexts, some of which may use clashing guava versions... For example, when explore launches a Hive query,
    // it includes hive-exec.jar which bundles guava 11 in its jar...
    Deque<File> stack = new ArrayDeque<>();
    stack.addAll(listFiles(directory));

    while (!stack.isEmpty()) {
      File file = stack.peekLast();
      List<File> files = listFiles(file);
      if (files.isEmpty()) {
        if (!file.delete()) {
          throw new IOException("Failed to delete file " + file);
        }
        stack.pollLast();
      } else {
        stack.addAll(files);
      }
    }

    if (!retain) {
      if (!directory.delete()) {
        throw new IOException("Failed to delete directory " + directory);
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

  /**
   * Returns list of file names under the given directory. An empty list will be returned if the given file is
   * not a directory.
   */
  public static List<String> list(File directory) {
    return listOf(directory.list());
  }

  /**
   * Returns list of file names under the given directory that are accepted by the given filter.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<String> list(File directory, FilenameFilter filenameFilter) {
    return listOf(directory.list(filenameFilter));
  }

  /**
   * Returns list of file names under the given directory that matches the give set of file name extension.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<String> list(File directory, String...extensions) {
    return list(directory, ImmutableSet.copyOf(extensions));
  }

  /**
   * Returns list of file names under the given directory that matches the give set of file name extension.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<String> list(File directory, Iterable<String> extensions) {
    final ImmutableSet<String> allowedExtensions = ImmutableSet.copyOf(extensions);

    return list(directory, new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return allowedExtensions.contains(Files.getFileExtension(name));
      }
    });
  }

  /**
   * Returns list of files under the given directory. An empty list will be returned if the
   * given file is not a directory.
   */
  public static List<File> listFiles(File directory) {
    return listOf(directory.listFiles());
  }

  /**
   * Returns list of files under the given directory that are accepted by the given filter.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<File> listFiles(File directory, FileFilter fileFilter) {
    return listOf(directory.listFiles(fileFilter));
  }

  /**
   * Returns list of files under the given directory that are accepted by the given filter.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<File> listFiles(File directory, FilenameFilter filenameFilter) {
    return listOf(directory.listFiles(filenameFilter));
  }

  /**
   * Returns list of files under the given directory that matches the give set of file name extension.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<File> listFiles(File directory, String...extensions) {
    return listFiles(directory, ImmutableSet.copyOf(extensions));
  }

  /**
   * Returns list of files under the given directory that matches the give set of file name extension.
   * An empty list will be returned if the given file is not a directory.
   */
  public static List<File> listFiles(File directory, Iterable<String> extensions) {
    final ImmutableSet<String> allowedExtensions = ImmutableSet.copyOf(extensions);

    return listFiles(directory, new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return allowedExtensions.contains(Files.getFileExtension(name));
      }
    });
  }

  /**
   * Converts the given array into a list. An empty list will be returned if the given array is {@code null}.
   * (Note: This method might worth to be in some other common class, which we don't have now).
   *
   * @param elements array to convert
   * @param <T> type of elements in the array
   * @return a new immutable list.
   */
  private static <T> List<T> listOf(@Nullable T[] elements) {
    return elements == null ? ImmutableList.<T>of() : ImmutableList.copyOf(elements);
  }
}
