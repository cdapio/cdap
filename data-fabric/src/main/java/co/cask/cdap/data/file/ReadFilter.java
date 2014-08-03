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
package co.cask.cdap.data.file;

/**
 * Filter for reading from {@link FileReader}.
 *
 * This class is experimental is still expanding.
 */
public abstract class ReadFilter {

  /**
   * Always accept what it sees.
   */
  public static final ReadFilter ALWAYS_ACCEPT = new ReadFilter() { };

  /**
   * Always reject offset.
   */
  public static final ReadFilter ALWAYS_REJECT_OFFSET = new ReadFilter() {
    @Override
    public boolean acceptOffset(long offset) {
      return false;
    }
  };

  /**
   * Accept or reject based on file offset.
   *
   * @param offset The file offset.
   * @return {@code true} to accept, {@code false} to reject.
   */
  public boolean acceptOffset(long offset) {
    return true;
  }

  /**
   * Accept or reject based on event timestamp.
   *
   * @param timestamp The timestamp of the event.
   * @return {@code true} to accept, {@code false} to reject.
   */
  public boolean acceptTimestamp(long timestamp) {
    return true;
  }
}
