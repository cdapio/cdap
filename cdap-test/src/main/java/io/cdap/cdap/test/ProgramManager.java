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

package io.cdap.cdap.test;

import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
   * Starts the program and waits for one additional run of the specified status. This is essentially a
   * call to {@link #start()} followed by a call to {@link #waitForRuns(ProgramRunStatus, int, long, TimeUnit)}.
   * It should not be used if there is another run in progress.
   *
   * @param status the status of the run to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @return the program manager itself
   */
  T startAndWaitForRun(ProgramRunStatus status, long timeout, TimeUnit timeoutUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Starts the program and waits for one additional run of the specified status. This is essentially a
   * call to {@link #start()} followed by a call to {@link #waitForRuns(ProgramRunStatus, int, long, TimeUnit)}.
   * Fails early if run gets into status that's {@link ProgramRunStatus#isUnsuccessful()}.
   * It should not be used if there is another run in progress.
   *
   * @param status the status of the run to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @return the program manager itself
   */
  T startAndWaitForGoodRun(ProgramRunStatus status, long timeout, TimeUnit timeoutUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Starts the program with arguments
   * @param arguments the arguments to start the program with
   * @return T the ProgramManager, itself
   */
  T start(Map<String, String> arguments);

  /**
   * Starts the program with arguments and waits for one additional run of the specified status.
   * This method assumes another run is not started by another thread. This is essentially a
   * call to {@link #start(Map)} ()} followed by a call to {@link #waitForRuns(ProgramRunStatus, int, long, TimeUnit)}.
   * It should not be used if there is another run in progress.
   *
   * @param arguments the arguments to start the program with
   * @param status the status of the run to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @return the program manager itself
   */
  T startAndWaitForRun(Map<String, String> arguments, ProgramRunStatus status, long timeout, TimeUnit timeoutUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Starts the program with arguments and waits for one additional run of the specified status.
   * Fails early if run gets into status that's {@link ProgramRunStatus#isUnsuccessful()}.
   * This method assumes another run is not started by another thread. This is essentially a
   * call to {@link #start(Map)} ()} followed by a call to {@link #waitForRuns(ProgramRunStatus, int, long, TimeUnit)}.
   * It should not be used if there is another run in progress.
   *
   * @param arguments the arguments to start the program with
   * @param status the status of the run to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @return the program manager itself
   */
  T startAndWaitForGoodRun(Map<String, String> arguments, ProgramRunStatus status, long timeout,
                           TimeUnit timeoutUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Starts the program with arguments and waits for one additional run of the specified status.
   * This method assumes another run is not started by another thread. This is essentially a
   * call to {@link #start(Map)} ()} followed by a call to {@link #waitForRuns(ProgramRunStatus, int, long, TimeUnit)}.
   * It should not be used if there is another run in progress.
   *
   * @param arguments the arguments to start the program with
   * @param status the status of the run to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @param sleepTime amount of time to sleep in between polls
   * @param sleepUnit sleep time unit type
   * @return the program manager itself
   */
  T startAndWaitForRun(Map<String, String> arguments, ProgramRunStatus status, long timeout, TimeUnit timeoutUnit,
                       long sleepTime, TimeUnit sleepUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Stops the program.
   */
  void stop();

  /**
   * Checks if program is running
   */
  boolean isRunning();

  /**
   * Wait for the status of the program with 5 seconds timeout. A started program does not mean all functionality
   * is available. For example, a started service may not yet have registered its url. In most cases,
   * {@link #waitForRun(ProgramRunStatus, long, TimeUnit)} should be used
   * with {@link ProgramRunStatus#RUNNING} to wait for a program to actually be running, and
   * {@link #waitForStopped(long, TimeUnit)} should be used to wait for the program to stop.
   *
   * @param status true if waiting for started, false if waiting for stopped.
   * @throws InterruptedException if the method is interrupted while waiting for the status.
   * @deprecated use {@link #waitForRun(ProgramRunStatus, long, TimeUnit)} or {@link #waitForStopped(long, TimeUnit)}.
   */
  @Deprecated
  void waitForStatus(boolean status) throws InterruptedException;

  /**
   * Wait for the program to be stopped.
   *
   * @throws InterruptedException if method is interrupted while waiting for runs
   * @throws TimeoutException if timeout reached
   * @throws ExecutionException if error getting program status
   */
  void waitForStopped(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException,
    ExecutionException;

  /**
   * Blocks until at least the one run record is available with the given {@link ProgramRunStatus}.
   *
   * @param status the status of the run record
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws InterruptedException if method is interrupted while waiting for runs
   * @throws TimeoutException if timeout reached
   * @throws ExecutionException if error getting runs
   */
  void waitForRun(ProgramRunStatus status, long timeout, TimeUnit timeoutUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Blocks until at least {@code runCount} number of run records with the given {@link ProgramRunStatus}.
   *
   * @param status the status of the run record
   * @param runCount the number of run records to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @throws InterruptedException if method is interrupted while waiting for runs
   * @throws TimeoutException if timeout reached
   * @throws ExecutionException if error getting runs
   */
  void waitForRuns(ProgramRunStatus status, int runCount, long timeout, TimeUnit timeoutUnit)
    throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Blocks until at least {@code runCount} number of run records with the given {@link ProgramRunStatus}.
   *
   * @param status the status of the run record
   * @param runCount the number of run records to wait for
   * @param timeout amount of time units to wait
   * @param timeoutUnit time unit type
   * @param sleepTime amount of time to sleep in between polls
   * @param sleepUnit sleep time unit type
   * @throws InterruptedException if method is interrupted while waiting for runs
   * @throws TimeoutException if timeout reached
   * @throws ExecutionException if error getting runs
   */
  void waitForRuns(ProgramRunStatus status, int runCount, long timeout, TimeUnit timeoutUnit, long sleepTime,
                   TimeUnit sleepUnit) throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Wait for the status of the program, retrying a given number of times with a timeout between attempts.
   * @param status true if waiting for started, false if waiting for stopped.
   * @param retries number of attempts to check for status.
   * @param timeout timeout in seconds between attempts.
   * @throws InterruptedException if the method is interrupted while waiting for the status.
   * @deprecated use {@link #waitForRun(ProgramRunStatus, long, TimeUnit)} or {@link #waitForStopped(long, TimeUnit)}.
   */
  @Deprecated
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

  /**
   * Save runtime arguments for the program for all runs.
   *
   * @param args the runtime arguments to save
   */
  void setRuntimeArgs(Map<String, String> args) throws Exception;

  /**
   * Get runtime arguments of this program
   *
   * @return args the runtime arguments of the programs
   */
  Map<String, String> getRuntimeArgs() throws Exception;
}
