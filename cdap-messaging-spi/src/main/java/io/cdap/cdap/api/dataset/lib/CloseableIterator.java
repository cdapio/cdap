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

package io.cdap.cdap.api.dataset.lib;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that can be closed.
 *
 * @param <T> Type of elements returned by this iterator
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

  /**
   * Returns an empty {@link CloseableIterator} that contains no element.
   */
  static <T> CloseableIterator<T> empty() {
    return new CloseableIterator<T>() {
      @Override
      public void close() {
        // no-op
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        throw new NoSuchElementException();
      }
    };
  }

  @Override
  void close();
}
