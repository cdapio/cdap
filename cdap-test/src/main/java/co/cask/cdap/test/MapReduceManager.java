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

package co.cask.cdap.test;

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.metrics.RuntimeMetrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Instance for this class is for managing a running {@link MapReduce}.
 */
public interface MapReduceManager {
  /**
   * Stops the running mapreduce job.
   */
  void stop();

  /**
   * Blocks until mapreduce job is finished or given timeout is reached
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws java.util.concurrent.TimeoutException if timeout reached
   * @throws InterruptedException if execution is interrupted
   */
  void waitForFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;
}
