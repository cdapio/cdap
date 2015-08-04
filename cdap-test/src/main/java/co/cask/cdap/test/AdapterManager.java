/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Instance of this class is for managing an adapter.
 */
public interface AdapterManager {

  /**
   * Starts the adapter.
   */
  void start() throws IOException;

  /**
   * Stops the adapter.
   */
  void stop() throws IOException;

  /**
   * Get the list of runs for the adapter.
   *
   * @return list of runs for the adapter
   */
  List<RunRecord> getRuns();

  /**
   * Get the run record for a specific run id.
   *
   * @param runId the id of the run to get
   */
  RunRecord getRun(String runId);

  /**
   * Blocks until a specific run of the adapter has the given status.
   *
   * @param runId the id of the run to wait for
   * @param status the status to block for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws TimeoutException if timeout reached
   * @throws InterruptedException if execution is interrupted
   */
  void waitForStatus(String runId, ProgramRunStatus status, long timeout,
                     TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Blocks until a one run of the adapter has finished. If a run has already finished this will return right away.
   * This should not be used on a Worker Adapter without calling {@link #stop()} first.
   *
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws TimeoutException if timeout reached
   * @throws InterruptedException if execution is interrupted
   */
  void waitForOneRunToFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Blocks until the specified number of runs of the adapter has finished.
   * If that many runs have already finished, this will return right away.
   * This should not be used on a Worker Adapter without calling {@link #stop()} first.
   *
   * @param numRuns the number of runs to wait to finish.
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws TimeoutException if timeout reached
   * @throws InterruptedException if execution is interrupted
   */
  void waitForRunsToFinish(int numRuns, long timeout,
                           TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Deletes the adapter.
   */
  void delete() throws Exception;
}
