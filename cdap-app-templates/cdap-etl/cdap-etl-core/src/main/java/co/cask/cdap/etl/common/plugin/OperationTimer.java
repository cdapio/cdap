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

package co.cask.cdap.etl.common.plugin;

/**
 * Measures how long operations take.
 * To time an operation, {@link #start()} should be called at the start of the operation, and {@link #reset()} should
 * be called at the end of the operation. If the stopwatch needs to be paused in the middle of the operation,
 * a call to {@link #start()} can be made, with a subsequent call to {@link #start()} to resume the stopwatch.
 */
public interface OperationTimer {

  /**
   * Starts the timer.
   *
   * @throws IllegalStateException if the timer is already running.
   */
  void start();

  /**
   * Stops the timer. Future reads will return the fixed duration that had elapsed up to this point.
   *
   * @throws IllegalStateException if the timer is already stopped.
   */
  void stop();

  /**
   * Resets the timer and updates the timing metrics.
   */
  void reset();
}
