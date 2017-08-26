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
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A default implementation of {@link RunRecordCorrectorService}.
 */
public class AbstractRunRecordCorrectorService extends AbstractIdleService implements RunRecordCorrectorService {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRunRecordCorrectorService.class);

  private final Store store;
  private final ProgramStateWriter programStateWriter;
  private final ProgramLifecycleService programLifecycleService;
  private final ProgramRuntimeService runtimeService;
  private final long startTimeoutSecs;

  @Inject
  AbstractRunRecordCorrectorService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                                    ProgramLifecycleService programLifecycleService,
                                    ProgramRuntimeService runtimeService) {
    this.store = store;
    this.programStateWriter = programStateWriter;
    this.programLifecycleService = programLifecycleService;
    this.runtimeService = runtimeService;
    this.startTimeoutSecs = 2L * cConf.getLong(Constants.AppFabric.PROGRAM_MAX_START_SECONDS);
  }

  void validateAndCorrectRunningRunRecords() {
    Set<String> processedInvalidRunRecordIds = Sets.newHashSet();

    // Lets update the running programs run records
    for (ProgramType programType : ProgramType.values()) {
      validateAndCorrectRunningRunRecords(programType, processedInvalidRunRecordIds);
    }

    if (!processedInvalidRunRecordIds.isEmpty()) {
      LOG.info("Corrected {} of run records with RUNNING or STARTING status but no actual program running.",
               processedInvalidRunRecordIds.size());
    }
  }

  /**
   * Fix all the possible inconsistent states for RunRecords that shows it is in RUNNING state but actually not
   * via check to {@link ProgramRuntimeService} for a type of CDAP program.
   *
   * @param programType The type of program the run records need to validate and update.
   * @param processedInvalidRunRecordIds the {@link Set} of processed invalid run record ids.
   */
  private void validateAndCorrectRunningRunRecords(final ProgramType programType,
                                                   final Set<String> processedInvalidRunRecordIds) {
    final Map<RunId, ProgramRuntimeService.RuntimeInfo> runIdToRuntimeInfo = runtimeService.list(programType);

    final long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    com.google.common.base.Predicate<RunRecordMeta> filter = new com.google.common.base.Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta input) {
        String runId = input.getPid();
        // runs are marked as starting before they get into the runtimeService list.
        // Therefore, add some buffer so that we don't incorrectly 'fix' runs that are actually starting
        long timeSinceStart = now - input.getStartTs();

        // Check if it is not actually running.
        return !runIdToRuntimeInfo.containsKey(RunIds.fromString(runId)) && timeSinceStart > startTimeoutSecs;
      }
    };
    LOG.trace("Start getting run records not actually running ...");
    List<RunRecordMeta> notActuallyRunning = new ArrayList<>();

    notActuallyRunning.addAll(store.getRuns(ProgramRunStatus.RUNNING, filter).values());
    LOG.trace("{} run records that are not actually running", notActuallyRunning.size());
    notActuallyRunning.addAll(store.getRuns(ProgramRunStatus.STARTING, filter).values());
    LOG.trace("{} run records that are not actually running or starting", notActuallyRunning.size());
    LOG.trace("End getting {} run records not actually running.", notActuallyRunning.size());

    final Map<String, ProgramId> runIdToProgramId = new HashMap<>();

    LOG.trace("Start getting invalid run records  ...");
    Collection<RunRecordMeta> invalidRunRecords =
      Collections2.filter(notActuallyRunning, new com.google.common.base.Predicate<RunRecordMeta>() {
        @Override
        public boolean apply(RunRecordMeta input) {
          String runId = input.getPid();
          // check for program Id for the run record, if null then it is invalid program type.
          ProgramId targetProgramId = programLifecycleService.retrieveProgramIdForRunRecord(programType, runId);

          // Check if run id is for the right program type
          if (targetProgramId != null) {
            runIdToProgramId.put(runId, targetProgramId);
            return true;
          } else {
            return false;
          }
        }
      });

    // don't correct run records for programs running inside a workflow
    // for instance, a MapReduce running in a Workflow will not be contained in the runtime info in this class
    invalidRunRecords = Collections2.filter(invalidRunRecords, new com.google.common.base.Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta invalidRunRecordMeta) {
        boolean shouldCorrect = shouldCorrectForWorkflowChildren(invalidRunRecordMeta, processedInvalidRunRecordIds);
        if (!shouldCorrect) {
          LOG.trace("Will not correct invalid run record {} since it's parent workflow still running.",
                    invalidRunRecordMeta);
          return false;
        }
        return true;
      }
    });

    LOG.trace("End getting invalid run records.");

    if (!invalidRunRecords.isEmpty()) {
      LOG.warn("Found {} RunRecords with RUNNING or STARTING status and the program not actually " +
                 "running or starting for program type {}",
               invalidRunRecords.size(), programType.getPrettyName());
    } else {
      LOG.trace("No RunRecords found with RUNNING or STARTING status and the program not actually " +
                  "running or starting for program type {}",
                programType.getPrettyName());
    }

    // Now lets correct the invalid RunRecords
    for (RunRecordMeta invalidRunRecordMeta : invalidRunRecords) {
      String runId = invalidRunRecordMeta.getPid();
      ProgramId targetProgramId = runIdToProgramId.get(runId);
      programStateWriter.error(targetProgramId.run(runId),
                               new Throwable("Marking run record as failed since no running program found."));
      LOG.warn("Fixed RunRecord {} for program {} with RUNNING or STARTING status because the program was not " +
                 "actually running or starting",
               runId, targetProgramId);

      processedInvalidRunRecordIds.add(runId);
    }
  }

  /**
   * Method to check if the run record is a child program of a Workflow
   *
   * @param runRecordMeta The target {@link RunRecordMeta} to check
   * @param processedInvalidRunRecordIds the {@link Set} of processed invalid run record ids.
   * @return {@code true} if we should check and {@code false} otherwise
   */
  private boolean shouldCorrectForWorkflowChildren(RunRecordMeta runRecordMeta,
                                                   Set<String> processedInvalidRunRecordIds) {
    // check if it is part of workflow because it may not have actual runtime info
    if (runRecordMeta.getProperties() != null && runRecordMeta.getProperties().get("workflowrunid") != null) {

      // Get the parent Workflow info
      String workflowRunId = runRecordMeta.getProperties().get("workflowrunid");
      if (!processedInvalidRunRecordIds.contains(workflowRunId)) {
        // If the parent workflow has not been processed, then check if it still valid
        ProgramId workflowProgramId = programLifecycleService.retrieveProgramIdForRunRecord(ProgramType.WORKFLOW,
                                                                                            workflowRunId);
        if (workflowProgramId != null) {
          // lets see if the parent workflow run records state is still running
          RunRecordMeta wfRunRecord = store.getRun(workflowProgramId, workflowRunId);
          ProgramRuntimeService.RuntimeInfo wfRuntimeInfo = runtimeService.lookup(workflowProgramId,
                                                                                  RunIds.fromString(workflowRunId));

          // Check of the parent workflow run record exists and it is running and runtime info said it is still there
          // then do not update it
          if (wfRunRecord != null && wfRunRecord.getStatus() == ProgramRunStatus.RUNNING && wfRuntimeInfo != null) {
            return false;
          }
        }
      }
    }

    return true;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting RunRecordCorrectorService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping RunRecordCorrectorService");
  }
}
