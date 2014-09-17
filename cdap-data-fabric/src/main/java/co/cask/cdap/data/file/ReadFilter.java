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
package co.cask.cdap.data.file;

/**
 * Filter for reading from {@link FileReader}.
 *
 * This class is experimental is still expanding.
 * <p/>
 * See {@link co.cask.cdap.data.stream.StreamDataFileWriter} for the file format.
 * <p/>
 * The methods on this class will be called in the following sequence:
 *
 * <pre>
 *  reset() - Called before filtering a new data block
 *  acceptTimestamp() - Called when the timestamp of a data block is read
 *  getNextTimestampHint() - Called if acceptTimestamp() return false
 *  acceptOffset() - Called at each stream event begin offset
 * </pre>
 *
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
   * Invoked before filtering a new data block.
   */
  public void reset() {
    // No-op by default.
  }

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


  /**
   * If the {@link #acceptTimestamp(long)} returns {@code false}, the filter can provide a hint for the next
   * timestamp the reader should start reading from.
   *
   * @return the timestamp for the next event should be or a negative value if no hint is provided.
   */
  public long getNextTimestampHint() {
    return -1L;
  }
}
