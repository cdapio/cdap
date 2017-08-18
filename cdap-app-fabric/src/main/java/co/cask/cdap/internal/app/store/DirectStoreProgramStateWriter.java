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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Supplier;
import com.google.inject.Inject;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of the ProgramStateWriter that persists directly to the store
 */
public final class DirectStoreProgramStateWriter implements ProgramStateWriter {
  private final Store store;

  @Inject
  public DirectStoreProgramStateWriter(Store store) {
    this.store = store;
  }

  @Override
  public void start(final ProgramRunId programRunId, final ProgramOptions programOptions, final String twillRunId) {
    // Get start time from RunId
    long startTime = RunIds.getTime(RunIds.fromString(programRunId.getRun()), TimeUnit.SECONDS);
    if (startTime == -1) {
      // If RunId is not time-based, use current time as start time
      startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }
    final long startTimeSeconds = startTime;
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setStart(programRunId.getParent(), programRunId.getRun(), startTimeSeconds,
                       twillRunId, programOptions.getUserArguments().asMap(), programOptions.getArguments().asMap());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void running(final ProgramRunId programRunId, final String twillRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setRunning(programRunId.getParent(), programRunId.getRun(),
                         TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                         twillRunId);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void completed(final ProgramRunId programRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setStop(programRunId.getParent(), programRunId.getRun(),
                      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED, null);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void killed(final ProgramRunId programRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setStop(programRunId.getParent(), programRunId.getRun(),
                      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.KILLED, null);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void error(final ProgramRunId programRunId, final Throwable failureCause) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setStop(programRunId.getParent(), programRunId.getRun(),
                      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                      ProgramRunStatus.FAILED, new BasicThrowable(failureCause));
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void suspend(final ProgramRunId programRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setSuspend(programRunId.getParent(), programRunId.getRun());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void resume(final ProgramRunId programRunId) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        store.setResume(programRunId.getParent(), programRunId.getRun());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }
}
