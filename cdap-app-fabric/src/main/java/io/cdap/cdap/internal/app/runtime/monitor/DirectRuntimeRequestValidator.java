/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RuntimeRequestValidator} implementation that reads from the runtime table directly.
 */
public final class DirectRuntimeRequestValidator implements RuntimeRequestValidator {

  private static final Logger LOG = LoggerFactory.getLogger(DirectRuntimeRequestValidator.class);

  private final TransactionRunner txRunner;
  private final ProgramRunRecordFetcher runRecordFetcher;
  private final LoadingCache<ProgramRunId, Optional<ProgramRunInfo>> programRunsCache;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  DirectRuntimeRequestValidator(CConfiguration cConf, TransactionRunner txRunner,
      ProgramRunRecordFetcher runRecordFetcher,
      AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext) {
    this.txRunner = txRunner;
    this.runRecordFetcher = runRecordFetcher;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;

    // Configure the cache with expiry the poll time.
    // This helps reduce the actual lookup for a burst of requests within one poll interval,
    // but not to keep it too long so that data becomes stale.
    long pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.programRunsCache = CacheBuilder.newBuilder()
        .expireAfterWrite(pollTimeMillis, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<ProgramRunId, Optional<ProgramRunInfo>>() {
          /**
           * For a programRunId this cache stores a {@link ProgramRunInfo} object only for valid programRunIds,
           * i.e. this value will be Optional.empty() if the programRunId is invalid.
           */
          @Override
          public Optional<ProgramRunInfo> load(ProgramRunId programRunId)
              throws IOException, UnauthorizedException {
            return getRunRecordStatusForProgramsInNonEndState(programRunId);
          }
        });
  }

  @Override
  public ProgramRunInfo getProgramRunStatus(ProgramRunId programRunId, HttpRequest request)
      throws BadRequestException, GoneException {
    accessEnforcer.enforce(programRunId, authenticationContext.getPrincipal(),
        StandardPermission.GET);
    ProgramRunInfo programRunInfo;
    try {
      programRunInfo = programRunsCache.get(programRunId)
          .orElseThrow(
              () -> new BadRequestException("Program run " + programRunId + " is not valid"));
    } catch (BadRequestException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceUnavailableException(Constants.Service.RUNTIME, e);
    }
    if (programRunInfo.getProgramRunStatus().isEndState()) {
      throw new GoneException(String.format("Program run %s status is %s",
          programRunId, programRunInfo.getProgramRunStatus()));
    }
    return programRunInfo;
  }

  private Optional<ProgramRunInfo> getRunRecordStatusForProgramsInNonEndState(
      ProgramRunId programRunId)
      throws IOException, UnauthorizedException {
    try {
      RunRecordDetail runRecord = TransactionRunners.run(txRunner, context -> {
        return AppMetadataStore.create(context).getRun(programRunId);
      }, IOException.class);

      if (runRecord != null) {
        return getValidRunRecordStatus(runRecord);
      }
      // If it is not found in the local store, which should be very rare, try to fetch the run record remotely.
      LOG.info("Remotely fetching program run details for {}", programRunId);
      runRecord = runRecordFetcher.getRunRecordMeta(programRunId);
      // Try to update the local store
      insertRunRecord(programRunId, runRecord);
      return runRecord.getStatus().isEndState() ? Optional.empty()
          : getValidRunRecordStatus(runRecord);
    } catch (NotFoundException e) {
      return Optional.empty();
    }
  }

  private Optional<ProgramRunInfo> getValidRunRecordStatus(RunRecordDetail runRecord) {
    ProgramRunStatus programRunStatus = runRecord.getStatus();
    if (programRunStatus != ProgramRunStatus.STOPPING) {
      return Optional.of(new ProgramRunInfo(programRunStatus));
    }

    // For stopping state, encode the termination timestamp in seconds
    long terminateTs = Optional.ofNullable(runRecord.getTerminateTs()).orElse(Long.MAX_VALUE);
    return Optional.of(new ProgramRunInfo(terminateTs));
  }

  /**
   * Inserts the given {@link RunRecordDetail} for the program run into the runtime store.
   */
  private void insertRunRecord(ProgramRunId programRunId, RunRecordDetail runRecord) {
    // For rejected run, don't need to record anything.
    if (runRecord.getStatus() == ProgramRunStatus.REJECTED) {
      return;
    }

    try {
      TransactionRunners.run(txRunner, context -> {
        AppMetadataStore store = AppMetadataStore.create(context);
        // Strip off user args and trim down system args as runtime only needs the run status for validation purpose.
        // User and system args could be large and store them in local store can lead to unnecessary storage
        // and processing overhead.
        store.recordProgramProvisioning(programRunId, Collections.emptyMap(),
            RuntimeMonitors.trimSystemArgs(runRecord.getSystemArgs()),
            runRecord.getSourceId(), runRecord.getArtifactId());
        store.recordProgramProvisioned(programRunId, 1, runRecord.getSourceId());
        store.recordProgramStart(programRunId, null, runRecord.getSystemArgs(),
            runRecord.getSourceId());
        store.recordProgramRunning(programRunId,
            Objects.firstNonNull(runRecord.getRunTs(), System.currentTimeMillis()),
            null, runRecord.getSourceId());
        switch (runRecord.getStatus()) {
          case SUSPENDED:
            store.recordProgramSuspend(programRunId, runRecord.getSourceId(),
                Objects.firstNonNull(runRecord.getSuspendTs(), System.currentTimeMillis()));
            break;
          case STOPPING:
            store.recordProgramStopping(programRunId, runRecord.getSourceId(),
                Objects.firstNonNull(runRecord.getStoppingTs(), System.currentTimeMillis()),
                // if terminate timestamp is null we will shut down gracefully
                Objects.firstNonNull(runRecord.getTerminateTs(), Long.MAX_VALUE));
            break;
          case COMPLETED:
          case KILLED:
          case FAILED:
            store.recordProgramStop(programRunId,
                Objects.firstNonNull(runRecord.getStopTs(), System.currentTimeMillis()),
                runRecord.getStatus(), null, runRecord.getSourceId());
            // We don't need to retain records for terminated programs, hence just delete it
            store.deleteRunIfTerminated(programRunId, runRecord.getSourceId());
            break;
        }
      }, IOException.class);
    } catch (Exception e) {
      // Don't throw if failed to update to the store. It doesn't affect normal operation.
      LOG.warn("Failed to update runtime store for program run {} with {}", programRunId, runRecord,
          e);
    }
  }
}
