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

package co.cask.cdap.data2.dataset2.tx;

import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
 * Handy implementation of tx context for {@link Transactional} that holds single dataset.
 *
 * @param <T> type of the dataset
 */
public class DatasetContext<T> implements Iterable<T> {
  private final T dataset;

  public static <TYPE> DatasetContext<TYPE> of(TYPE dateset) {
    return new DatasetContext<>(dateset);
  }

  public DatasetContext(T dataset) {
    this.dataset = dataset;
  }

  public T get() {
    return dataset;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.singletonIterator(dataset);
  }
}
