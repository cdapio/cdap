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

package co.cask.cdap.api.data.batch;

/**
 * Provides an abstract implementation of {@link SplitReader}.
 * <p>
 *   Iterates over split data using the {@link #fetchNextKeyValue()} method.
 * </p>
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public abstract class SplitReaderBase<KEY, VALUE> extends SplitReader<KEY, VALUE> {
  private KEY currentKey;
  private VALUE currentValue;

  /**
   * Fetches the next data item of the split being read. 
   *
   * If true, use the {@link #setCurrentKeyValue(Object, Object)} method to
   * set the new current key/value. If false there are no more key/value records to read. 
   *
   * See {@link co.cask.cdap.api.data.batch.IteratorBasedSplitReader} for an implementation 
   * of the abstract fetchNextKeyValue() method.
   *
   * @return false if reached end of the split, true otherwise.
   */
  protected abstract boolean fetchNextKeyValue();

  protected void setCurrentKeyValue(KEY key, VALUE value) {
    currentKey = key;
    currentValue = value;
  }

  @Override
  public boolean nextKeyValue() throws InterruptedException {
    return fetchNextKeyValue();
  }

  @Override
  public KEY getCurrentKey() throws InterruptedException {
    return currentKey;
  }

  @Override
  public VALUE getCurrentValue() throws InterruptedException {
    return currentValue;
  }

  @Override
  public void close() {
  }
}
