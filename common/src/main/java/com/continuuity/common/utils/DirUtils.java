package com.continuuity.common.utils;

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
}
