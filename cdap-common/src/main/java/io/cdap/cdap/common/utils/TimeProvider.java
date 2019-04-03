/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An interface provide time.
 */
public interface TimeProvider {

  /**
   * A {@link TimeProvider} that provides timestamp using system clock by calling {@link System#currentTimeMillis()}.
   */
  TimeProvider SYSTEM_TIME = new TimeProvider() {
    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  };

  /**
   * A {@link TimeProvider} that provides timestamp in incremental fashion. It returns the {@code start} value
   * as provided to the constructor for the first call,
   * followed by {@code start + 1, start + 2, start + 3, ...} and so on for subsequent calls.
   */
  final class IncrementalTimeProvider implements TimeProvider {

    private final AtomicLong ticks;

    /**
     * Creates a new instance that starts with {@code 0}.
     */
    public IncrementalTimeProvider() {
      this(0);
    }

    /**
     * Creates a new instance that starts with the given value.
     */
    public IncrementalTimeProvider(long start) {
      this.ticks = new AtomicLong(start);
    }

    @Override
    public long currentTimeMillis() {
      return ticks.getAndIncrement();
    }
  }

  /**
   * Returns the current time in milliseconds.
   */
  long currentTimeMillis();
}
