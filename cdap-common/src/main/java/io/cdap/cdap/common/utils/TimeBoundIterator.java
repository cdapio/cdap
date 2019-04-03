/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.common.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

/**
 * An iterator that will act as if there are no more elements if a certain amount of time has passed, or there actually
 * are no more elements.
 *
 * @param <T> type of element in the iterator
 */
public class TimeBoundIterator<T> extends AbstractIterator<T> {
  private final Iterator<T> delegate;
  private final long timeBoundMillis;
  private final Stopwatch stopwatch;

  public TimeBoundIterator(Iterator<T> delegate, long timeBoundMillis) {
    this(delegate, timeBoundMillis, new Stopwatch());
  }

  @VisibleForTesting
  TimeBoundIterator(Iterator<T> delegate, long timeBoundMillis, Stopwatch stopwatch) {
    this.delegate = delegate;
    this.timeBoundMillis = timeBoundMillis;
    this.stopwatch = stopwatch;
    this.stopwatch.start();
  }

  @Override
  protected T computeNext() {
    if (stopwatch.elapsedMillis() < timeBoundMillis && delegate.hasNext()) {
      return delegate.next();
    }
    return endOfData();
  }
}
