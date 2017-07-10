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

import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Supplier;
import org.apache.twill.api.RunId;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An implementation of the ProgramStateWriter that immediately persists to the store
 */
public final class ProgramStorePublisher implements ProgramStateWriter {
  private final RuntimeStore runtimeStore;
  private final ProgramId programId;
  private final RunId runId;
  private final String twillRunId;
  private final Arguments userArguments;
  private final Arguments systemArguments;

  public ProgramStorePublisher(ProgramId programId, RunId runId, String twillRunId,
                               Arguments userArguments, Arguments systemArguments, RuntimeStore runtimeStore) {
    this.programId = programId;
    this.runId = runId;
    this.twillRunId = twillRunId;
    this.userArguments = userArguments;
    this.systemArguments = systemArguments;
    this.runtimeStore = runtimeStore;
  }

  @Override
  public void start(final long startTime) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setInit(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(startTime), twillRunId,
                             userArguments.asMap(), systemArguments.asMap());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void running(final long startTimeInSeconds) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setStart(programId, runId.getId(), startTimeInSeconds, twillRunId,
                              userArguments.asMap(), systemArguments.asMap());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void stop(final long endTime, final ProgramRunStatus runStatus, final @Nullable BasicThrowable cause) {
    if (programId.getType() == ProgramType.MAPREDUCE) {
      System.out.println("MR PROGRAM " + programId + " HAS STATUS " + runStatus);
    }
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setStop(programId, runId.getId(), TimeUnit.MILLISECONDS.toSeconds(endTime), runStatus, cause);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void suspend() {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setSuspend(programId, runId.getId());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  @Override
  public void resume() {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setResume(programId, runId.getId());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }
}
