/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.StageMetrics;

import java.util.Iterator;

/**
 * Wrapper around an Iterator that emits metrics whenever an item is fetched from the iterator.
 *
 * @param <T> type of object in the iterator
 */
public class TrackedIterator<T> implements Iterator<T> {
  private final Iterator<T> delegate;
  private final StageMetrics stageMetrics;
  private final String metricName;

  public TrackedIterator(Iterator<T> delegate, StageMetrics stageMetrics, String metricName) {
    this.delegate = delegate;
    this.stageMetrics = stageMetrics;
    this.metricName = metricName;
  }

  @Override
  public boolean hasNext() {
    return delegate.hasNext();
  }

  @Override
  public T next() {
    stageMetrics.count(metricName, 1);
    return delegate.next();
  }

  @Override
  public void remove() {
    delegate.remove();
  }
}
