/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 * Iterator for iterating over index entries of {@link StreamDataFileIndex}.
 */
interface StreamDataFileIndexIterator {

  /**
   * Reads the next index entry.
   *
   * @return {@code true} if index entry is read, {@code false} otherwise.
   */
  boolean nextIndexEntry();

  /**
   * Returns the timestamp of current entry.
   */
  long currentTimestamp();

  /**
   * Returns the position of current entry.
   */
  long currentPosition();
}
