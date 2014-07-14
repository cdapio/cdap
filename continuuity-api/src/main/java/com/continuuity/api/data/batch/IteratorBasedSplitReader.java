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

package com.continuuity.api.data.batch;

import java.util.Iterator;

/**
 * Handy implementation of {@link SplitReader} backed by {@link Iterator}.
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public abstract class IteratorBasedSplitReader<KEY, VALUE>
  extends SplitReaderBase<KEY, VALUE> {
  private Iterator<VALUE> iterator;

  /**
   * Creates an iterator to iterate through all records of a given split.
   * @param split Split to iterate through.
   * @return An instance of {@link Iterator}.
   */
  protected abstract Iterator<VALUE> createIterator(Split split);

  /**
   * Gets the key belonging to the key/value record provided by the iterator.
   * @param value The value of the key/value record.
   * @return The key of the key/value record.
   */
  protected abstract KEY getKey(VALUE value);

  @Override
  public void initialize(final Split split) throws InterruptedException {

    iterator = createIterator(split);
  }

  @Override
  protected boolean fetchNextKeyValue() {
    if (!iterator.hasNext()) {
      return false;
    } else {
      VALUE next = iterator.next();
      KEY key = getKey(next);
      setCurrentKeyValue(key, next);
      return true;
    }
  }
}
