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
 * Abstract implementation of {@link WorkerManager} that includes common functionality for all implementations.
 */
public abstract class AbstractWorkerManager implements WorkerManager {

  @Override
  public void waitForStatus(boolean status) throws InterruptedException {
    waitForStatus(status, 5, 1);
  }

  @Override
  public void waitForStatus(boolean status, int retries, int timeout) throws InterruptedException {
    int trial = 0;
    while (trial++ < retries) {
      if (isRunning() == status) {
        return;
      }
      TimeUnit.SECONDS.sleep(timeout);
    }
    throw new IllegalStateException("Worker state not executed. Expected " + status);
  }
}
