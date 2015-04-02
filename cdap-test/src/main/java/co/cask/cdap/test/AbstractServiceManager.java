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

import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation of {@link ServiceManager} that includes common functionality for all implementations.
 */
public abstract class AbstractServiceManager implements ServiceManager {

  @Override
  public void waitForStatus(boolean status) throws InterruptedException {
    waitForStatus(status, 50, 5000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void waitForStatus(boolean status, int retries, int timeout) throws InterruptedException {
    // The API is not good. The "timeout" parameter is actually sleep time
    // We calculate the actual timeout by multiplying retries with sleep time
    long sleepTimeMillis = TimeUnit.SECONDS.toMillis(timeout);
    long timeoutMillis = sleepTimeMillis * retries;
    waitForStatus(status, sleepTimeMillis, timeoutMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits for the program status.
   *
   * @param status true to wait for start, false for stop
   * @param sleepTime time to sleep in between polling
   * @param timeout timeout for the polling if the status doesn't match
   * @param timeoutUnit unit for both sleepTime and timeout
   */
  private void waitForStatus(boolean status, long sleepTime,
                             long timeout, TimeUnit timeoutUnit) throws InterruptedException {
    long timeoutMillis = timeoutUnit.toMillis(timeout);
    boolean statusMatched = status == isRunning();
    long startTime = System.currentTimeMillis();
    while (!statusMatched && (System.currentTimeMillis() - startTime) < timeoutMillis) {
      timeoutUnit.sleep(sleepTime);
      statusMatched = status == isRunning();
    }

    if (!statusMatched) {
      throw new IllegalStateException("Service state not executed. Expected " + status);
    }

  }
}
