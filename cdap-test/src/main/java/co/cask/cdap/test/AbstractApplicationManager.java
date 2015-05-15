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
import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Instance of this class is for managing deployed application.
 */
public abstract class AbstractApplicationManager implements ApplicationManager {

  protected abstract boolean isRunning(Id.Program programId);

  protected void programWaitForFinish(long timeout, TimeUnit timeoutUnit,
                                      Id.Program programId) throws InterruptedException, TimeoutException {
    programWaitForState(timeout, timeoutUnit, false, programId);
  }

  protected void programWaitForRunning(long timeout, TimeUnit timeoutUnit,
                                       Id.Program programId) throws InterruptedException, TimeoutException {
    programWaitForState(timeout, timeoutUnit, true, programId);
  }

  protected void programWaitForState(long timeout, TimeUnit timeoutUnit, boolean waitForRunning,
                                     Id.Program programId) throws InterruptedException, TimeoutException {
    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = Math.max(10, Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
    Stopwatch stopwatch = new Stopwatch().start();
    while (isRunning(programId) != waitForRunning && stopwatch.elapsedTime(timeoutUnit) < timeout) {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
    }

    if (isRunning(programId) != waitForRunning) {
      throw new TimeoutException("Time limit reached.");
    }
  }
}
