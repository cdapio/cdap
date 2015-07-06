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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Service that manages lifecycle of Programs.
 */
public class ProgramLifecycleService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleService.class);

  private final ScheduledExecutorService scheduledExecutorService;
  private final Store store;
  private final ProgramRuntimeService runtimeService;

  @Inject
  public ProgramLifecycleService(Store store, ProgramRuntimeService runtimeService) {
    this.store = store;
    this.runtimeService = runtimeService;
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ProgramLifecycleService");

    scheduledExecutorService.scheduleWithFixedDelay(new RunRecordsCorrectorRunnable(this),
                                                    2L, 600L, TimeUnit.SECONDS);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down ProgramLifecycleService");

    scheduledExecutorService.shutdown();
    try {
      if (!scheduledExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduledExecutorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  private Program getProgram(Id.Program id) throws IOException, ProgramNotFoundException {
    Program program = store.loadProgram(id);
    if (program == null) {
      throw new ProgramNotFoundException(id);
    }
    return program;
  }

  /**
   * Start a Program.
   *
   * @param id {@link Id.Program}
   * @param systemArgs system arguments
   * @param userArgs user arguments
   * @param debug enable debug mode
   * @return {@link ProgramRuntimeService.RuntimeInfo}
   * @throws IOException if there is an error starting the program
   * @throws ProgramNotFoundException if program is not found
   */
  public ProgramRuntimeService.RuntimeInfo start(final Id.Program id, Map<String, String> systemArgs,
                                                 Map<String, String> userArgs, boolean debug)
    throws IOException, ProgramNotFoundException {
    final String adapterName = systemArgs.get(ProgramOptionConstants.ADAPTER_NAME);
    Program program = getProgram(id);
    BasicArguments systemArguments = new BasicArguments(systemArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.run(program, new SimpleProgramOptions(
      id.getId(), systemArguments, userArguments, debug));

    final ProgramController controller = runtimeInfo.getController();
    final String runId = controller.getRunId().getId();
    final String twillRunId = runtimeInfo.getTwillRunId() == null ? null : runtimeInfo.getTwillRunId().getId();
    if (id.getType() != ProgramType.MAPREDUCE && id.getType() != ProgramType.SPARK) {
      // MapReduce state recording is done by the MapReduceProgramRunner
      // TODO [JIRA: CDAP-2013] Same needs to be done for other programs as well
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State state, @Nullable Throwable cause) {
          // Get start time from RunId
          long startTimeInSeconds = RunIds.getTime(controller.getRunId(), TimeUnit.SECONDS);
          if (startTimeInSeconds == -1) {
            // If RunId is not time-based, use current time as start time
            startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          }
          store.setStart(id, runId, startTimeInSeconds, adapterName, twillRunId);
          if (state == ProgramController.State.COMPLETED) {
            completed();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }

        @Override
        public void completed() {
          LOG.debug("Program {} {} {} completed successfully.", id.getNamespaceId(), id.getApplicationId(), id.getId());
          store.setStop(id, runId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.COMPLETED.getRunStatus());
        }

        @Override
        public void killed() {
          LOG.debug("Program {} {} {} killed.", id.getNamespaceId(), id.getApplicationId(), id.getId());
          store.setStop(id, runId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.KILLED.getRunStatus());
        }

        @Override
        public void suspended() {
          LOG.debug("Suspending Program {} {} {} {}.", id.getNamespaceId(), id.getApplicationId(), id, runId);
          store.setSuspend(id, runId);
        }

        @Override
        public void resuming() {
          LOG.debug("Resuming Program {} {} {} {}.", id.getNamespaceId(), id.getApplicationId(), id, runId);
          store.setResume(id, runId);
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", id, runId, cause);
          store.setStop(id, runId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.ERROR.getRunStatus());
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }
    return runtimeInfo;
  }

  /**
   * Stop a Program given its {@link RunId}.
   *
   * @param programId The id of the program
   * @param runId {@link RunId} of the program
   * @throws ExecutionException
   * @throws InterruptedException
   */
  //TODO: Improve this once we have logic moved from ProgramLifecycleHttpHandler for stopping a program
  public void stopProgram(Id.Program programId, RunId runId) throws ExecutionException, InterruptedException {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.lookup(programId, runId);
    if (runtimeInfo != null) {
      runtimeInfo.getController().stop().get();
    } else {
      LOG.warn("RunTimeInfo not found for Program {} RunId {} to be stopped", programId, runId);
    }
  }

  /**
   * Returns runtime information for the given program if it is running,
   * or {@code null} if no instance of it is running.
   *
   * @param programId {@link Id.Program}
   * @param programType {@link ProgramType}
   */
  public ProgramRuntimeService.RuntimeInfo findRuntimeInfo(Id.Program programId, ProgramType programType) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(programType).values();
    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  /**
   * Fix all the possible inconsistent states for RunRecords that shows it is in RUNNING state but actually not
   * via check to {@link ProgramRuntimeService}.
   */
  private void validateAndCorrectRunningRunRecords() {
    Set<String> processedInvalidRunRecordIds = Sets.newHashSet();

    // Lets update the running programs run records
    for (ProgramType programType : ProgramType.values()) {
      validateAndCorrectRunningRunRecords(programType, processedInvalidRunRecordIds);
    }

    if (!processedInvalidRunRecordIds.isEmpty()) {
      LOG.info("Corrected {} of run records with RUNNING status but no actual program running.",
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
  void validateAndCorrectRunningRunRecords(ProgramType programType, Set<String> processedInvalidRunRecordIds) {
    final Map<RunId, RuntimeInfo> runIdToRuntimeInfo = runtimeService.list(programType);

    List<RunRecord> invalidRunRecords = store.getRuns(ProgramRunStatus.RUNNING, new Predicate<RunRecord>() {
      @Override
      public boolean apply(@Nullable RunRecord input) {
        if (input == null) {
          return false;
        }
        // Check if it is actually running
        String runId = input.getPid();
        return !runIdToRuntimeInfo.containsKey(RunIds.fromString(runId));
      }
    });

    if (!invalidRunRecords.isEmpty()) {
      LOG.warn("Found {} RunRecords with RUNNING status but the program not actually running",
               invalidRunRecords.size());
    }

    // Now lets correct the invalid RunRecords
    for (RunRecord rr : invalidRunRecords) {
      String runId = rr.getPid();
      Id.Program targetProgramId = retrieveProgramIdForRunRecord(programType, runId);
      if (targetProgramId == null) {
        // wrong program type
        continue;
      }

      boolean shouldCorrect = shouldCorrectForWorkflowChildren(rr, processedInvalidRunRecordIds);
      if (!shouldCorrect) {
        continue;
      }

      LOG.warn("Fixing RunRecord {} in program {} of type {} with RUNNING status but the program is not running",
               runId, targetProgramId, programType.getPrettyName());

      store.compareAndSetStatus(targetProgramId, runId, ProgramController.State.ALIVE.getRunStatus(),
                                ProgramController.State.ERROR.getRunStatus());

      processedInvalidRunRecordIds.add(runId);
    }
  }

  /**
   * Helper method to check if the run record is a child program of a Workflow
   *
   * @param runRecord The target {@link RunRecord} to check
   * @param processedInvalidRunRecordIds the {@link Set} of processed invalid run record ids.
   * @return {@code true} of we should check and {@code false} otherwise
   */
  private boolean shouldCorrectForWorkflowChildren(RunRecord runRecord, Set<String> processedInvalidRunRecordIds) {
    // check if it is part of workflow because it may not have actual runtime info
    if (runRecord.getProperties() != null && runRecord.getProperties().get("workflowrunid") != null) {

      // Get the parent Workflow info
      String workflowRunId = runRecord.getProperties().get("workflowrunid");
      if (!processedInvalidRunRecordIds.contains(workflowRunId)) {
        // If the parent workflow has not been processed, then check if it still valid
        Id.Program workflowProgramId = retrieveProgramIdForRunRecord(ProgramType.WORKFLOW, workflowRunId);
        if (workflowProgramId != null) {
          // lets see if the parent workflow run records state is still running
          RunRecord wfRunRecord = store.getRun(workflowProgramId, workflowRunId);
          RuntimeInfo wfRuntimeInfo = runtimeService.lookup(workflowProgramId, RunIds.fromString(workflowRunId));

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

  /**
   * Helper method to get {@link co.cask.cdap.proto.Id.Program} for a RunRecord for type of program
   *
   * @param programType Type of program to search
   * @param runId The target id of the {@link RunRecord} to find
   * @return the program id of the run record or {@code null} if does not exist.
   */
  @Nullable
  private Id.Program retrieveProgramIdForRunRecord(ProgramType programType, String runId) {

    // Get list of namespaces (borrow logic from AbstractAppFabricHttpHandler#listPrograms)
    List<NamespaceMeta> namespaceMetas = store.listNamespaces();

    // For each, get all programs under it
    Id.Program targetProgramId = null;
    for (NamespaceMeta nm : namespaceMetas) {
      Id.Namespace accId = Id.Namespace.from(nm.getName());
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(accId);

      // For each application get the programs checked against run records
      for (ApplicationSpecification appSpec : appSpecs) {
        switch (programType) {
          case FLOW:
            for (String programName : appSpec.getFlows().keySet()) {
              Id.Program programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                 programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case MAPREDUCE:
            for (String programName : appSpec.getMapReduce().keySet()) {
              Id.Program programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                 programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case SPARK:
            for (String programName : appSpec.getSpark().keySet()) {
              Id.Program programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                 programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case SERVICE:
            for (String programName : appSpec.getServices().keySet()) {
              Id.Program programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                 programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case WORKER:
            for (String programName : appSpec.getWorkers().keySet()) {
              Id.Program programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                 programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case WORKFLOW:
            for (String programName : appSpec.getWorkflows().keySet()) {
              Id.Program programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                 programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          default:
            LOG.debug("Unknown program type: " + programType.name());
            break;
        }
        if (targetProgramId != null) {
          break;
        }
      }
      if (targetProgramId != null) {
        break;
      }
    }

    return targetProgramId;
  }

  /**
   * Helper method to get program id for a run record if it exists in the store.
   *
   * @return instance of {@link Id.Program} if exist for the runId or null if does not.
   */
  @Nullable
  private Id.Program validateProgramForRunRecord(String namespaceName, String appName, ProgramType programType,
                                                 String programName, String runId) {
    Id.Program programId = Id.Program.from(namespaceName, appName, programType, programName);
    RunRecord runRecord = store.getRun(programId, runId);
    if (runRecord != null) {
      return programId;
    } else {
      return null;
    }
  }

  /**
   * Helper class to run in separate thread to validate the invalid running run records
   */
  public static class RunRecordsCorrectorRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RunRecordsCorrectorRunnable.class);

    private final ProgramLifecycleService programLifecycleService;

    public RunRecordsCorrectorRunnable(ProgramLifecycleService programLifecycleService) {
      this.programLifecycleService = programLifecycleService;
    }

    @Override
    public void run() {
      try {
        RunRecordsCorrectorRunnable.LOG.debug("Start correcting invalid run records ...");

        // Lets update the running programs run records
        programLifecycleService.validateAndCorrectRunningRunRecords();

        RunRecordsCorrectorRunnable.LOG.debug("End correcting invalid run records.");
      } catch (Throwable t) {
        // Ignore any exception thrown since this behaves like daemon thread.
        LOG.debug("Exception thrown when running run id cleaner.", t);
      }
    }
  }

}
