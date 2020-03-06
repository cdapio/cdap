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


package io.cdap.cdap.internal.app.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.runtime.LocalDatasetDeleterRunnable;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * The base implementation of the service that periodically scans for program runs that no longer running.
 */
public abstract class RunRecordCorrectorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RunRecordCorrectorService.class);
  // program runs can possibly be in these states but not be present in the ProgramRuntimeService.
  // for example, if CDAP is shut down during a program run, then the program is killed on YARN,
  // when CDAP starts back up, the program run will be one of these, but will never transition out without correction.
  // PENDING is not in this list because provision tasks run in the CDAP master and are resumed on start up.
  // So if CDAP is shut down in the middle of a provision task, it will get resumed by the ProvisioningService.
  // RESUMING in not in this set because it is not actually a state, despite being an enum value.
  private static final Set<ProgramRunStatus> NOT_STOPPED_STATUSES = EnumSet.of(ProgramRunStatus.STARTING,
                                                                               ProgramRunStatus.RUNNING,
                                                                               ProgramRunStatus.SUSPENDED);
  private final Store store;
  private final ProgramStateWriter programStateWriter;
  private final ProgramRuntimeService runtimeService;
  private final long startTimeoutSecs;
  private final int txBatchSize;
  private final CConfiguration cConf;
  private final NamespaceAdmin namespaceAdmin;
  private final DatasetFramework datasetFramework;
  private ScheduledExecutorService localDatasetDeleterService;

  RunRecordCorrectorService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                            ProgramRuntimeService runtimeService, NamespaceAdmin namespaceAdmin,
                            DatasetFramework datasetFramework) {
    this(cConf, store, programStateWriter, runtimeService, namespaceAdmin, datasetFramework,
         2L * cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS),
         cConf.getInt(Constants.AppFabric.PROGRAM_RUNID_CORRECTOR_TX_BATCH_SIZE));
  }

  @VisibleForTesting
  RunRecordCorrectorService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                            ProgramRuntimeService runtimeService, NamespaceAdmin namespaceAdmin,
                            DatasetFramework datasetFramework,
                            long startTimeoutSecs, int txBatchSize) {
    this.store = store;
    this.programStateWriter = programStateWriter;
    this.runtimeService = runtimeService;
    this.cConf = cConf;
    this.namespaceAdmin = namespaceAdmin;
    this.datasetFramework = datasetFramework;

    this.startTimeoutSecs = startTimeoutSecs;
    this.txBatchSize = txBatchSize;
  }

  void fixRunRecords() {
    Set<ProgramRunId> fixed = doFixRunRecords();

    if (!fixed.isEmpty()) {
      LOG.info("Corrected {} run records with status in {} that have no actual running program. " +
                 "Such programs likely have crashed or were killed by external signal.",
               fixed.size(), NOT_STOPPED_STATUSES);
    }
  }

  /**
   * Fix all the possible inconsistent states for RunRecords that shows it is in RUNNING state but actually not
   * via check to {@link ProgramRuntimeService} for a type of CDAP program.
   *
   * @return the set of fixed {@link ProgramRunId}.
   */
  private Set<ProgramRunId> doFixRunRecords() {
    LOG.trace("Start getting run records not actually running ...");
    // Get run records in STARTING, RUNNING and SUSPENDED states that are actually not running
    // Do it in micro batches of transactions to avoid tx timeout

    Set<ProgramRunId> fixedPrograms = new HashSet<>();
    Predicate<RunRecordDetail> filter = createFilter(fixedPrograms);
    for (ProgramRunStatus status : NOT_STOPPED_STATUSES) {
      while (true) {
        // runs are not guaranteed to come back in order of start time, so need to scan the entire time range
        // each time. Should not be worse in performance than specifying a more restrictive time range
        // because time range is just used as a read-time filter.
        Map<ProgramRunId, RunRecordDetail> runs = store.getRuns(status, 0L, Long.MAX_VALUE, txBatchSize, filter);
        LOG.trace("{} run records in {} state but are not actually running", runs.size(), status);
        if (runs.isEmpty()) {
          break;
        }

        for (RunRecordDetail record : runs.values()) {
          ProgramRunId programRunId = record.getProgramRunId();
          String msg = String.format(
            "Fixed RunRecord for program run %s in %s state because it is actually not running",
            programRunId, record.getStatus());
          programStateWriter.error(programRunId, new ProgramRunAbortedException(msg));
          fixedPrograms.add(programRunId);
          LOG.warn(msg);
        }
      }
    }

    if (fixedPrograms.isEmpty()) {
      LOG.trace("No RunRecord found with status in {}, but the program are not actually running", NOT_STOPPED_STATUSES);
    } else {
      LOG.warn("Fixed {} RunRecords with status in {}, but the programs are not actually running",
               fixedPrograms.size(), NOT_STOPPED_STATUSES);
    }
    return fixedPrograms;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting RunRecordCorrectorService");

    localDatasetDeleterService = Executors
      .newSingleThreadScheduledExecutor(r -> new Thread(r, "local dataset deleter"));
    long interval = cConf.getLong(Constants.AppFabric.LOCAL_DATASET_DELETER_INTERVAL_SECONDS);
    if (interval <= 0) {
      LOG.warn("Invalid interval specified for the local dataset deleter {}. Setting it to 3600 seconds.", interval);
      interval = 3600L;
    }

    long initialDelay = cConf.getLong(Constants.AppFabric.LOCAL_DATASET_DELETER_INITIAL_DELAY_SECONDS);
    if (initialDelay <= 0) {
      LOG.warn("Invalid initial delay specified for the local dataset deleter {}. Setting it to 300 seconds.",
               initialDelay);
      initialDelay = 300L;
    }

    Runnable runnable = new LocalDatasetDeleterRunnable(namespaceAdmin, store, datasetFramework);
    localDatasetDeleterService.scheduleWithFixedDelay(runnable, initialDelay, interval, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping RunRecordCorrectorService");

    localDatasetDeleterService.shutdown();
    try {
      if (!localDatasetDeleterService.awaitTermination(5, TimeUnit.SECONDS)) {
        localDatasetDeleterService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Creates a {@link Predicate} for {@link RunRecordDetail} to be used for scanning the {@link Store} for run records
   * that doesn't have the program actually running as determined by the {@link ProgramRuntimeService}.
   *
   * @param excludedIds a set of {@link ProgramRunId} that are always rejected by the filter.
   */
  private Predicate<RunRecordDetail> createFilter(Set<ProgramRunId> excludedIds) {
    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    return record -> {
      ProgramRunId programRunId = record.getProgramRunId();

      if (excludedIds.contains(programRunId)) {
        return false;
      }

      // Don't fix ClusterMode.ISOLATED runs that are not in STARTING state, since the runtime monitor should handle it.
      // For programs in STARTING state, if it doesn't transit to other state, we should consider it as stuck.
      String clusterMode = record.getSystemArgs().get(ProgramOptionConstants.CLUSTER_MODE);
      // Use equals instead of valueOf to handle null for old records and also potential future name change
      if (record.getStatus() != ProgramRunStatus.STARTING && ClusterMode.ISOLATED.name().equals(clusterMode)) {
        return false;
      }

      // When a program starts, a STARTING state will be record into the Store and it can happen before
      // the insertion of RuntimeInfo into the ProgramRuntimeService, since writing to Store and launching program
      // happens asynchronously.
      // Therefore, add some time buffer so that we don't incorrectly 'fix' runs that are actually starting
      long timeSinceStart = now - record.getStartTs();

      ProgramId programId = programRunId.getParent();

      // If it is running inside workflow, check if the workflow is running.
      // If the workflow is still running, then we don't fix this run record.
      // This is because it won't be found via the ProgramRuntimeService
      Map<String, String> systemArgs = record.getSystemArgs();
      String workflowName = systemArgs == null ? null : systemArgs.get(ProgramOptionConstants.WORKFLOW_NAME);
      String workflowRunId = systemArgs == null ? null : systemArgs.get(ProgramOptionConstants.WORKFLOW_RUN_ID);
      if (workflowName != null && workflowRunId != null) {
        ProgramId workflowProgramId = programId.getParent().program(ProgramType.WORKFLOW, workflowName);

        // If the workflow is still running, ignore the record.
        if (runtimeService.list(workflowProgramId).containsKey(RunIds.fromString(workflowRunId))) {
          return false;
        }
      }

      // Check if it is not actually running.
      return timeSinceStart > startTimeoutSecs
        && !runtimeService.list(programId).containsKey(RunIds.fromString(programRunId.getRun()));

    };
  }
}
