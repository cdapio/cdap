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

package co.cask.cdap.api.data.batch;

/**
 * Defines a reader of a dataset {@link Split}.
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public abstract class SplitReader<KEY, VALUE> {

  /**
   * Called once at initialization.
   * @param split The split that defines the range of records to read.
   * @throws InterruptedException
   */
  public abstract void initialize(Split split) throws InterruptedException;

  /**
   * Read the next key, value pair.
   * @return true if a key/value pair was read.
   * @throws InterruptedException
   */
  public abstract boolean nextKeyValue() throws InterruptedException;

  /**
   * Get the current key.
   * @return The current key, or null if there is no current key.
   * @throws InterruptedException
   */
  public abstract KEY getCurrentKey() throws InterruptedException;

  /**
   * Get the current value.
   * @return The current value of the object that was read.
   * @throws InterruptedException
   */
  public abstract VALUE getCurrentValue() throws InterruptedException;

  /**
   * The current progress of the record reader through its data.
   * By default progress is not reported in the middle of split reading.
   * @return A number between 0.0 and 1.0 that is the fraction of the data that has been read.
   * @throws InterruptedException
   */
  public float getProgress() throws InterruptedException {
    return 0;
  }

  /**
   * Close the record reader.
   */
  public abstract void close();
}
