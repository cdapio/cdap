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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Instance for this class is for managing a {@link Id.Program}.
 * @param <T> The type of ProgramManager
 */
public interface ProgramManager<T extends ProgramManager> {

  /**
   * Starts the program
   * @return T The ProgramManager, itself
   */
  T start();

  /**
   * Starts the program with arguments
   * @param arguments the arguments to start the program with
   * @return T the ProgramManager, itself
   */
  T start(Map<String, String> arguments);

  /**
   * Stops the program.
   */
  void stop();

  /**
   * Checks if program is running
   */
  boolean isRunning();

  /**
   * Blocks until program is finished or given timeout is reached
   *
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws java.util.concurrent.TimeoutException if timeout reached
   * @throws InterruptedException if execution is interrupted
   */
  void waitForFinish(long timeout, TimeUnit timeoutUnit) throws TimeoutException, InterruptedException;

  /**
   * Wait for the status of the program with 5 seconds timeout.
   * @param status true if waiting for started, false if waiting for stopped.
   * @throws InterruptedException if the method is interrupted while waiting for the status.
   */
  void waitForStatus(boolean status) throws InterruptedException;

  /**
   * Wait for the status of the program, retrying a given number of times with a timeout between attempts.
   * @param status true if waiting for started, false if waiting for stopped.
   * @param retries number of attempts to check for status.
   * @param timeout timeout in seconds between attempts.
   * @throws InterruptedException if the method is interrupted while waiting for the status.
   */
  void waitForStatus(boolean status, int retries, int timeout) throws InterruptedException;

  /**
   * Gets the history of the program
   * @return list of {@link RunRecord} history
   */
  List<RunRecord> getHistory();

  /**
   * Gets the history of the program
   * @return list of {@link RunRecord} history
   */
  List<RunRecord> getHistory(ProgramRunStatus status);
}
