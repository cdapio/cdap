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

/**
 * Managing the worker in an application.
 */
public interface WorkerManager {

  /**
   * Changes the number of instances.
   *
   * @param instances number of instances to change to.
   */
  void setRunnableInstances(int instances);

  /**
   * Stops the running worker.
   */
  void stop();

  /**
   * Checks if the worker is running.
   */
  boolean isRunning();

  /**
   * Wait for the status of the Worker with default retries of 5 and a timeout of 1 second between retry attempts.
   * @param status true if waiting for started, false if waiting for stopped
   * @throws InterruptedException if the method is interrupted while waiting for the status
   */
  void waitForStatus(boolean status) throws InterruptedException;

  /**
   * Wait for the status of the Worker, retrying a given number of times with a timeout between attempts.
   * @param status true if waiting for started, false if waiting for stopped
   * @param retries number of attempts to check for status
   * @param timeout timeout in seconds between attempts
   * @throws InterruptedException if the methods is interrupted while waiting for the status
   */
  void waitForStatus(boolean status, int retries, int timeout) throws InterruptedException;
}
