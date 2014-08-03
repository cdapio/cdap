/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.data.stream;

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
