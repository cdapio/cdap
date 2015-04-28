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
import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Base implementation of {@link ApplicationManager}.
 */
public abstract class AbstractAdapterManager implements AdapterManager {

  @Override
  public void waitForStatus(String runId, ProgramRunStatus status, long timeout,
                            TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
    RunRecord runRecord = getRun(runId);
    // Min sleep time is 10ms, max sleep time is 1 seconds
    long sleepMillis = getSleepTime(timeout, timeoutUnit);

    Stopwatch stopwatch = new Stopwatch().start();
    while (runRecord.getStatus() != status && stopwatch.elapsedTime(timeoutUnit) < timeout) {
      TimeUnit.MILLISECONDS.sleep(sleepMillis);
      runRecord = getRun(runId);
    }

    if (runRecord.getStatus() != status) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  @Override
  public void waitForOneRunToFinish(long timeout,
                                    TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
    waitForRunsToFinish(1, timeout, timeoutUnit);
  }

  @Override
  public void waitForRunsToFinish(int numRuns, long timeout,
                                  TimeUnit timeoutUnit) throws TimeoutException, InterruptedException {
    long sleepMillis = getSleepTime(timeout, timeoutUnit);
    int numFinished = 0;

    Stopwatch stopwatch = new Stopwatch().start();
    while (stopwatch.elapsedTime(timeoutUnit) < timeout) {
      numFinished = 0;
      for (RunRecord record : getRuns()) {
        if (record.getStatus() != ProgramRunStatus.RUNNING) {
          numFinished++;
        }
      }
      if (numFinished < numRuns) {
        TimeUnit.MILLISECONDS.sleep(sleepMillis);
      } else {
        break;
      }
    }

    if (numFinished < numRuns) {
      throw new TimeoutException("Time limit reached.");
    }
  }

  private long getSleepTime(long timeout, TimeUnit timeoutUnit) {
    // Min sleep time is 10ms, max sleep time is 1 seconds
    return Math.max(10, Math.min(timeoutUnit.toMillis(timeout) / 10, TimeUnit.SECONDS.toMillis(1)));
  }
}
