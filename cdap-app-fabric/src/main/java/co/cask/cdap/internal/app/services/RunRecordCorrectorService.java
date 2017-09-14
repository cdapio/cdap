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


package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.LocalDatasetDeleterRunnable;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The base implementation of the service that periodically scans for program runs that no longer running.
 */
public abstract class RunRecordCorrectorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RunRecordCorrectorService.class);
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
    Predicate<RunRecordMeta> filter = createFilter(fixedPrograms);
    for (ProgramRunStatus status : NOT_STOPPED_STATUSES) {
      long startTime = 0L;
      while (true) {
        Map<ProgramRunId, RunRecordMeta> runs = store.getRuns(status, startTime, Long.MAX_VALUE, txBatchSize, filter);
        LOG.trace("{} run records in {} state but are not actually running", runs.size());
        if (runs.isEmpty()) {
          break;
        }

        for (RunRecordMeta record : runs.values()) {
          startTime = Math.max(startTime, RunIds.getTime(record.getPid(), TimeUnit.SECONDS));

          ProgramRunId programRunId = record.getProgramRunId();
          if (!fixedPrograms.contains(programRunId)) {
            String msg = String.format(
              "Fixed RunRecord for program run %s in %s state because it is actually not running",
              programRunId, record.getStatus());

            programStateWriter.error(programRunId, new ProgramRunAbortedException(msg));
            fixedPrograms.add(programRunId);
            LOG.warn(msg);
          }
        }
      }
    }

    if (fixedPrograms.isEmpty()) {
      LOG.trace("No RunRecord found with status in {} and the program not actually running", NOT_STOPPED_STATUSES);
    } else {
      LOG.warn("Fixed {} RunRecords with status in {} and the program not actually running",
               fixedPrograms.size(), NOT_STOPPED_STATUSES);
    }
    return fixedPrograms;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting RunRecordCorrectorService");

    localDatasetDeleterService = Executors.newScheduledThreadPool(1);
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
   * Creates a {@link Predicate} for {@link RunRecordMeta} to be used for scanning the {@link Store} for run records
   * that doesn't have the program actually running as determined by the {@link ProgramRuntimeService}.
   *
   * @param excludedIds a set of {@link ProgramRunId} that are always rejected by the filter.
   */
  private Predicate<RunRecordMeta> createFilter(final Set<ProgramRunId> excludedIds) {
    final long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    return new Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta record) {
        ProgramRunId programRunId = record.getProgramRunId();

        if (excludedIds.contains(programRunId)) {
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
      }
    };
  }
}
