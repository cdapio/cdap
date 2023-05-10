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
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
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
  private final long terminateTimeBufferSecs;
  private final long tetheredProgramTerminateTimeSecs;
  private final int numThreads;
  private ExecutorService executorService;
  private final MetricsCollectionService metricsCollectionService;
  private final ProgramStateWriter programStateWriter;

  @Inject
  ProgramRunStatusMonitorService(CConfiguration cConf, Store store, ProgramRuntimeService runtimeService,
                                 MetricsCollectionService metricsCollectionService,
                                 ProgramStateWriter programStateWriter) {
    this(cConf, store, runtimeService, metricsCollectionService, programStateWriter,
         cConf.getInt(Constants.AppFabric.PROGRAM_TERMINATOR_TX_BATCH_SIZE),
         cConf.getLong(Constants.AppFabric.PROGRAM_TERMINATOR_INTERVAL_SECS),
         cConf.getLong(Constants.AppFabric.PROGRAM_TERMINATE_TIME_BUFFER_SECS),
         cConf.getLong(Constants.AppFabric.TETHERED_PROGRAM_TERMINATE_TIME_SECS));
  }

  @VisibleForTesting
  ProgramRunStatusMonitorService(CConfiguration cConf, Store store, ProgramRuntimeService runtimeService,
                                 MetricsCollectionService metricsCollectionService,
                                 ProgramStateWriter programStateWriter, int txBatchSize,
                                 long specifiedIntervalSecs, long terminateTimeBufferSecs,
                                 long tetheredProgramTerminateTimeSecs) {
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
    this.terminateTimeBufferSecs = terminateTimeBufferSecs;
    this.numThreads = cConf.getInt(Constants.AppFabric.PROGRAM_KILL_THREADS);
    this.metricsCollectionService = metricsCollectionService;
    this.programStateWriter = programStateWriter;
    this.tetheredProgramTerminateTimeSecs = tetheredProgramTerminateTimeSecs;
  }

  @Override
  protected void doStartUp() throws Exception {
    super.doStartUp();
    ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads, 60, TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<>(),
                                                         Threads.createDaemonThreadFactory("program-kill-%d"));
    executor.allowCoreThreadTimeOut(true);
    this.executorService = executor;
  }

  @Override
  protected void doShutdown() throws Exception {
    super.doShutdown();
    if (executorService != null) {
      executorService.shutdown();
    }
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
  @VisibleForTesting
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
        ProgramRunStatus.STOPPING, 0L, currentTimeInSecs - terminateTimeBufferSecs, txBatchSize, filter);
      if (stoppingRuns.isEmpty()) {
        break;
      }
      for (RunRecordDetail record : stoppingRuns.values()) {
        String provisionerName = SystemArguments.getProfileProvisioner(record.getSystemArgs());
        ProgramRunId programRunId = record.getProgramRunId();
        // If this run was initiated by this CDAP instance to run on another instance, it will use the tethering
        // provisioner. The instance that actually runs it on the other side will have a non-null peer name.
        if (record.getPeerName() == null && TetheringProvisioner.TETHERING_NAME.equals(provisionerName)) {
          LOG.info("Forcing the termination of tethered program run {} as it should have stopped at {} ",
                   programRunId, record.getTerminateTs());
          programScannedForTermination.add(programRunId);
          programStateWriter.killed(programRunId);
          emitForceTerminatedRunsMetric(programRunId, record);
          continue;
        }

        programScannedForTermination.add(programRunId);
        RuntimeInfo runtimeInfo = runtimeService.lookup(programRunId.getParent(),
                                                        RunIds.fromString(programRunId.getRun()));
        if (runtimeInfo != null && runtimeInfo.getController() != null) {
          executorService.submit(() -> {
            LOG.info("Forcing the termination of program run {} as it should have stopped at {} ",
                     programRunId, record.getTerminateTs());
            runtimeInfo.getController().kill();
            emitForceTerminatedRunsMetric(programRunId, record);
          });
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
      Long stoppingTime = record.getStoppingTs();
      if (terminateTime == null) {
        // should not happen when state is STOPPING
        return false;
      }
      String provisionerName = SystemArguments.getProfileProvisioner(record.getSystemArgs());
      // If this run was initiated by this CDAP instance to run on another instance, it will use the tethering
      // provisioner. The instance that actually runs it on the other side will have a non-null peer name.
      if (record.getPeerName() == null && TetheringProvisioner.TETHERING_NAME.equals(provisionerName)) {
        if (stoppingTime == null) {
          // should not happen when state is STOPPING
          return false;
        }
        // Give additional grace period (= tetheredProgramTerminateTimeSecs) for tethered programs running on another
        // instance to terminate.
        return currentTimeInSecs - tetheredProgramTerminateTimeSecs > stoppingTime &&
          terminateTime <= currentTimeInSecs;
      }
      return terminateTime <= currentTimeInSecs;
    };
  }

  private void emitForceTerminatedRunsMetric(ProgramRunId programRunId, RunRecordDetail recordedRunRecord) {
    Optional<ProfileId> profile = SystemArguments.getProfileIdFromArgs(programRunId.getNamespaceId(),
                                                                       recordedRunRecord.getSystemArgs());
    Map<String, String> additionalTags = new HashMap<>();
    // don't want to add the tag if it is not present otherwise it will result in NPE
    additionalTags.computeIfAbsent(Constants.Metrics.Tag.PROVISIONER,
                                   provisioner -> SystemArguments.getProfileProvisioner(
                                     recordedRunRecord.getSystemArgs()));
    profile.ifPresent(profileId -> {
      emitProfileMetrics(programRunId, profileId, additionalTags);
    });
  }

  private void emitProfileMetrics(ProgramRunId programRunId, ProfileId profileId,
                                  Map<String, String> additionalTags) {
    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.PROFILE_SCOPE, profileId.getScope().name())
      .put(Constants.Metrics.Tag.PROFILE, profileId.getProfile())
      .put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace())
      .put(Constants.Metrics.Tag.PROGRAM, programRunId.getProgram())
      .put(Constants.Metrics.Tag.APP, programRunId.getApplication())
      .putAll(additionalTags)
      .build();
    metricsCollectionService.getContext(tags).increment(Constants.Metrics.Program.PROGRAM_FORCE_TERMINATED_RUNS, 1L);
  }
}
