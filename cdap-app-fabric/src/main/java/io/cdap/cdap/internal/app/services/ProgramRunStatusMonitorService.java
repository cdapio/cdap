/*
 * Copyright © 2022 Cask Data, Inc.
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


package io.cdap.cdap.internal.app.services;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.inject.Inject;

/**
 * A service that periodically scans the store for program runs that are in Stopping state and
 * force terminates them if they are running beyond the expected terminateTime.
 */
public class ProgramRunStatusMonitorService extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunStatusMonitorService.class);

  private final Store store;
  private final ProgramRuntimeService runtimeService;
  private final int txBatchSize;
  private final long intervalMillis;

  @Inject
  ProgramRunStatusMonitorService(CConfiguration cConf, Store store, ProgramRuntimeService runtimeService) {
    this(cConf, store, runtimeService, cConf.getInt(Constants.AppFabric.PROGRAM_TERMINATOR_TX_BATCH_SIZE),
         cConf.getLong(Constants.AppFabric.PROGRAM_TERMINATOR_INTERVAL_SECS));
  }

  @VisibleForTesting
  ProgramRunStatusMonitorService(CConfiguration cConf, Store store, ProgramRuntimeService runtimeService,
                                 int txBatchSize, long specifiedIntervalSecs) {
    super(RetryStrategies.fromConfiguration(cConf, Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX));
    this.store = store;
    this.runtimeService = runtimeService;
    this.txBatchSize = txBatchSize;
    if (specifiedIntervalSecs <= 0) {
      LOG.warn("Invalid interval {} specified for the program terminator. Setting it to 5 minutes.",
               specifiedIntervalSecs);
      specifiedIntervalSecs = TimeUnit.MINUTES.toSeconds(5);
    }
    this.intervalMillis = TimeUnit.SECONDS.toMillis(specifiedIntervalSecs);
  }

  @Override
  protected long runTask() {
    Set<ProgramRunId> programsScannedForTermination = terminatePrograms();
    if (!programsScannedForTermination.isEmpty()) {
      LOG.info("{} programs with STOPPING status that were active beyond their graceful shutdown period" +
                 " were attempted to be terminated.", programsScannedForTermination.size());
    }
    return this.intervalMillis;
  }

  /**
   * This method scans the store for program runs that are in STOPPING state and checks their terminateTime.
   * If terminateTime is in the past but the program is still active, then it stops the program.
   *
   * @return a set of programRunIds for which program termination was attempted
   */
  Set<ProgramRunId> terminatePrograms() {
    // fetch all runs that are in stopping state that started at most a minute before current time.
    // Specifying the entire time range should not be worse in performance
    // than specifying a more restrictive time range because time range is just used as a read-time filter
    long currentTimeInSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    Set<ProgramRunId> programScannedForTermination = new HashSet<>();
    Predicate<RunRecordDetail> filter = createFilter(currentTimeInSecs, programScannedForTermination);
    while (true) {
      // runs are not guaranteed to come back in order of start time, so need to scan the entire time range
      // each time. Should not be worse in performance than specifying a more restrictive time range
      // because time range is just used as a read-time filter.
      Map<ProgramRunId, RunRecordDetail> stoppingRuns = store.getRuns(
        ProgramRunStatus.STOPPING, 0L, currentTimeInSecs - TimeUnit.MINUTES.toSeconds(1), txBatchSize, filter);
      if (stoppingRuns.isEmpty()) {
        break;
      }
      for (RunRecordDetail record : stoppingRuns.values()) {
        ProgramRunId programRunId = record.getProgramRunId();
        programScannedForTermination.add(programRunId);
        RuntimeInfo runtimeInfo = runtimeService.lookup(programRunId.getParent(),
                                                        RunIds.fromString(programRunId.getRun()));
        if (runtimeInfo != null && runtimeInfo.getController() != null) {
          LOG.info("Forcing the termination of program {} as it should have stopped at {} ",
                   programRunId, record.getTerminateTs());
          runtimeInfo.getController().stop();
        }
      }
    }
    return programScannedForTermination;
  }

  private Predicate<RunRecordDetail> createFilter(long currentTimeInSecs, Set<ProgramRunId> excludedIds) {
    return record -> {
      ProgramRunId programRunId = record.getProgramRunId();
      if (excludedIds.contains(programRunId)) {
        return false;
      }
      Long terminateTime = record.getTerminateTs();
      if (terminateTime == null || terminateTime > currentTimeInSecs) {
        return false;
      }
      return true;
    };
  }
}
