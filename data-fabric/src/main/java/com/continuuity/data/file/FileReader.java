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
package com.continuuity.data.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * This interface represents classes that can read data from file.
 *
 * @param <T> Type of data can be read.
 * @param <P> Type of position object.
 */
public interface FileReader<T, P> extends Closeable, PositionReporter<P> {

  /**
   * Initialize the file reader.
   *
   * @throws IOException If initialization failed.
   */
  void initialize() throws IOException;

  /**
   * Reads as much data as possible until maxEvents.
   *
   * @param events Collection for storing data read.
   * @param maxEvents Maximum number of events to read.
   * @param timeout Maximum of time to spend on trying to read events
   * @param unit Unit for the timeout.
   *
   * @return Number of events read, could be {@code 0}. If {@code -1} is returned, meaning it reached EOF and all
   *         sub-sequence call to this method will also return {@code -1}.
   * @throws IOException If failed to read.
   * @throws InterruptedException If the reading is interrupted.
   */
  int read(Collection<? super T> events, int maxEvents,
           long timeout, TimeUnit unit) throws IOException, InterruptedException;

  /**
   * Reads as much data as possible that passes the given filter until maxEvents.
   *
   * @param events Collection for storing data read.
   * @param maxEvents Maximum number of events to read.
   * @param timeout Maximum of time to spend on trying to read events
   * @param unit Unit for the timeout.
   * @param readFilter Filter to apply during reading
   *
   * @return Number of events read, could be {@code 0}. If {@code -1} is returned, meaning it reached EOF and all
   *         sub-sequence call to this method will also return {@code -1}.
   * @throws IOException If failed to read.
   * @throws InterruptedException If the reading is interrupted.
   */
  int read(Collection<? super T> events, int maxEvents,
           long timeout, TimeUnit unit, ReadFilter readFilter) throws IOException, InterruptedException;
}
