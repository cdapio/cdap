/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
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

  private static final Gson GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  private final ScheduledExecutorService scheduledExecutorService;
  private final Store store;
  private final ProgramRuntimeService runtimeService;
  private final CConfiguration cConf;
  private final NamespaceStore nsStore;
  private final PropertiesResolver propertiesResolver;
  private final PreferencesStore preferencesStore;
  private final AuthorizerInstantiator authorizerInstantiator;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  @Inject
  ProgramLifecycleService(Store store, NamespaceStore nsStore, ProgramRuntimeService runtimeService,
                          CConfiguration cConf, PropertiesResolver propertiesResolver,
                          NamespacedLocationFactory namespacedLocationFactory, PreferencesStore preferencesStore,
                          AuthorizerInstantiator authorizerInstantiator,
                          AuthorizationEnforcer authorizationEnforcer,
                          AuthenticationContext authenticationContext) {
    this.store = store;
    this.nsStore = nsStore;
    this.runtimeService = runtimeService;
    this.propertiesResolver = propertiesResolver;
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
    this.cConf = cConf;
    this.preferencesStore = preferencesStore;
    this.authorizerInstantiator = authorizerInstantiator;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ProgramLifecycleService");

    long interval = cConf.getLong(Constants.AppFabric.PROGRAM_RUNID_CORRECTOR_INTERVAL_SECONDS);
    if (interval <= 0) {
      LOG.debug("Invalid run id corrector interval {}. Setting it to 180 seconds.", interval);
      interval = 180L;
    }
    scheduledExecutorService.scheduleWithFixedDelay(new RunRecordsCorrectorRunnable(this),
                                                    2L, interval, TimeUnit.SECONDS);
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

  /**
   * Returns the program status.
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  public ProgramStatus getProgramStatus(ProgramId programId) throws Exception {
    // check that app exists
    ApplicationSpecification appSpec = store.getApplication(programId.toId().getApplication());
    if (appSpec == null) {
      throw new NotFoundException(Ids.namespace(programId.getNamespace()).app(programId.getApplication()).toId());
    }

    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);

    if (runtimeInfo == null) {
      if (programId.getType() != ProgramType.WEBAPP) {
        //Runtime info not found. Check to see if the program exists.
        ProgramSpecification spec = getProgramSpecification(programId);
        if (spec == null) {
          // program doesn't exist
          throw new NotFoundException(programId);
        }
        ensureAccess(programId);

        if ((programId.getType() == ProgramType.MAPREDUCE || programId.getType() == ProgramType.SPARK) &&
          !store.getRuns(programId.toId(), ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, 1).isEmpty()) {
          // MapReduce program exists and running as a part of Workflow
          return ProgramStatus.RUNNING;
        }
        return ProgramStatus.STOPPED;
      }
    }

    return runtimeInfo.getController().getState().getProgramStatus();
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   *
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  public ProgramSpecification getProgramSpecification(ProgramId programId) throws Exception {
    ApplicationSpecification appSpec;
    appSpec = store.getApplication(Ids.namespace(programId.getNamespace()).app(programId.getApplication()).toId());
    if (appSpec == null) {
      return null;
    }

    ensureAccess(programId);
    String programName = programId.getProgram();
    ProgramType type = programId.getType();
    ProgramSpecification programSpec;
    if (type == ProgramType.FLOW && appSpec.getFlows().containsKey(programName)) {
      programSpec = appSpec.getFlows().get(programName);
    } else if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(programName)) {
      programSpec = appSpec.getMapReduce().get(programName);
    } else if (type == ProgramType.SPARK && appSpec.getSpark().containsKey(programName)) {
      programSpec = appSpec.getSpark().get(programName);
    } else if (type == ProgramType.WORKFLOW && appSpec.getWorkflows().containsKey(programName)) {
      programSpec = appSpec.getWorkflows().get(programName);
    } else if (type == ProgramType.SERVICE && appSpec.getServices().containsKey(programName)) {
      programSpec = appSpec.getServices().get(programName);
    } else if (type == ProgramType.WORKER && appSpec.getWorkers().containsKey(programName)) {
      programSpec = appSpec.getWorkers().get(programName);
    } else {
      programSpec = null;
    }
    return programSpec;
  }

  /**
   * Starts a Program with the specified argument overrides.
   *
   * @param programId the {@link ProgramId} to start/stop
   * @param overrides the arguments to override in the program's configured user arguments before starting
   * @param debug {@code true} if the program is to be started in debug mode, {@code false} otherwise
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link Action#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public synchronized void start(ProgramId programId, Map<String, String> overrides, boolean debug) throws Exception {
    if (isRunning(programId) && !isConcurrentRunsAllowed(programId.getType())) {
      throw new ConflictException(String.format("Program %s is already running", programId));
    }

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(programId.toId());
    Map<String, String> userArgs = propertiesResolver.getUserProperties(programId.toId());
    if (overrides != null) {
      userArgs.putAll(overrides);
    }

    ProgramRuntimeService.RuntimeInfo runtimeInfo = start(programId, sysArgs, userArgs, debug);
    if (runtimeInfo == null) {
      throw new IOException(String.format("Failed to start program %s", programId));
    }
  }

  /**
   * Start a Program.
   *
   * @param programId  the {@link ProgramId program} to start
   * @param systemArgs system arguments
   * @param userArgs user arguments
   * @param debug enable debug mode
   * @return {@link ProgramRuntimeService.RuntimeInfo}
   * @throws IOException if there is an error starting the program
   * @throws ProgramNotFoundException if program is not found
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link Action#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public ProgramRuntimeService.RuntimeInfo start(final ProgramId programId, final Map<String, String> systemArgs,
                                                 final Map<String, String> userArgs, boolean debug) throws Exception {
    authorizerInstantiator.get().enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    ProgramDescriptor programDescriptor = store.loadProgram(programId.toId());
    BasicArguments systemArguments = new BasicArguments(systemArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.run(programDescriptor, new SimpleProgramOptions(
      programId.getProgram(), systemArguments, userArguments, debug));

    final ProgramController controller = runtimeInfo.getController();
    final String runId = controller.getRunId().getId();
    final String twillRunId = runtimeInfo.getTwillRunId() == null ? null : runtimeInfo.getTwillRunId().getId();
    if (programId.getType() != ProgramType.MAPREDUCE && programId.getType() != ProgramType.SPARK) {
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
          store.setStart(programId.toId(), runId, startTimeInSeconds, twillRunId, userArgs, systemArgs);
          if (state == ProgramController.State.COMPLETED) {
            completed();
          }
          if (state == ProgramController.State.ERROR) {
            error(controller.getFailureCause());
          }
        }

        @Override
        public void completed() {
          LOG.debug("Program {} completed successfully.", programId);
          store.setStop(programId.toId(), runId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.COMPLETED.getRunStatus());
        }

        @Override
        public void killed() {
          LOG.debug("Program {} killed.", programId);
          store.setStop(programId.toId(), runId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.KILLED.getRunStatus());
        }

        @Override
        public void suspended() {
          LOG.debug("Suspending Program {} {}.", programId, runId);
          store.setSuspend(programId.toId(), runId);
        }

        @Override
        public void resuming() {
          LOG.debug("Resuming Program {} {}.", programId, runId);
          store.setResume(programId.toId(), runId);
        }

        @Override
        public void error(Throwable cause) {
          LOG.info("Program stopped with error {}, {}", programId, runId, cause);
          store.setStop(programId.toId(), runId, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                        ProgramController.State.ERROR.getRunStatus(), new BasicThrowable(cause));
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }
    return runtimeInfo;
  }

  /**
   * Stops the specified program. The first run of the program as found by {@link ProgramRuntimeService} is stopped.
   *
   * @param programId the {@link ProgramId program} to stop
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws InterruptedException if there was a problem while waiting for the stop call to complete
   * @throws ExecutionException if there was a problem while waiting for the stop call to complete
   */
  public synchronized void stop(ProgramId programId) throws Exception {
    stop(programId, null);
  }

  /**
   * Stops the specified run of the specified program.
   *
   * @param programId the {@link ProgramId program} to stop
   * @param runId the runId of the program run to stop. If null, the first run of the program as returned by
   *              {@link ProgramRuntimeService} is stopped.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws InterruptedException if there was a problem while waiting for the stop call to complete
   * @throws ExecutionException if there was a problem while waiting for the stop call to complete
   */
  public void stop(ProgramId programId, @Nullable String runId) throws Exception {
    issueStop(programId, runId).get();
  }

  /**
   * Issues a command to stop the specified {@link RunId} of the specified {@link ProgramId} and returns a
   * {@link ListenableFuture} with the {@link ProgramController} for it.
   * Clients can wait for completion of the {@link ListenableFuture}.
   *
   * @param programId the {@link ProgramId program} to issue a stop for
   * @param runId the runId of the program run to stop. If null, the first run of the program as returned by
   *              {@link ProgramRuntimeService} is stopped.
   * @return a {@link ListenableFuture} with a {@link ProgramController} that clients can wait on for stop to complete.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws UnauthorizedException if the user issuing the command is not authorized to stop the program. To stop a
   *                               program, a user requires {@link Action#EXECUTE} permission on the program.
   */
  public ListenableFuture<ProgramController> issueStop(ProgramId programId, @Nullable String runId) throws Exception {
    authorizerInstantiator.get().enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId);
    if (runtimeInfo == null) {
      if (!store.applicationExists(programId.toId().getApplication())) {
        throw new ApplicationNotFoundException(programId.toId().getApplication());
      } else if (!store.programExists(programId.toId())) {
        throw new ProgramNotFoundException(programId.toId());
      } else if (runId != null) {
        ProgramRunId programRunId = programId.run(runId);
        // Check if the program is running and is started by the Workflow
        RunRecordMeta runRecord = store.getRun(programId.toId(), runId);
        if (runRecord != null && runRecord.getProperties().containsKey("workflowrunid")
          && runRecord.getStatus().equals(ProgramRunStatus.RUNNING)) {
          String workflowRunId = runRecord.getProperties().get("workflowrunid");
          throw new BadRequestException(String.format("Cannot stop the program '%s' started by the Workflow " +
                                                        "run '%s'. Please stop the Workflow.", programRunId,
                                                      workflowRunId));
        }
        throw new NotFoundException(programRunId);
      }
      throw new BadRequestException(String.format("Program '%s' is not running.", programId));
    }
    return runtimeInfo.getController().stop();
  }

  /**
   * Save runtime arguments for all future runs of this program. The runtime arguments are saved in the
   * {@link PreferencesStore}.
   *
   * @param programId the {@link ProgramId program} for which runtime arguments are to be saved
   * @param runtimeArgs the runtime arguments to save
   * @throws NotFoundException if the specified program was not found
   * @throws UnauthorizedException if the current user does not have sufficient privileges to save runtime arguments for
   *                               the specified program. To save runtime arguments for a program, a user requires
   *                               {@link Action#ADMIN} privileges on the program.
   */
  public void saveRuntimeArgs(ProgramId programId, Map<String, String> runtimeArgs) throws Exception {
    authorizerInstantiator.get().enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!store.programExists(programId.toId())) {
      throw new NotFoundException(programId.toId());
    }

    preferencesStore.setProperties(programId.getNamespace(), programId.getApplication(),
                                   programId.getType().getCategoryName(),
                                   programId.getProgram(), runtimeArgs);
  }

  private boolean isRunning(ProgramId programId) throws Exception {
    return ProgramStatus.STOPPED != getProgramStatus(programId);
  }

  private boolean isConcurrentRunsAllowed(ProgramType type) {
    // Concurrent runs are only allowed for the Workflow and MapReduce
    return EnumSet.of(ProgramType.WORKFLOW, ProgramType.MAPREDUCE).contains(type);
  }

  @Nullable
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId,
                                                            @Nullable String runId) throws BadRequestException {
    Map<RunId, ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(programId.getType());

    if (runId != null) {
      RunId run;
      try {
        run = RunIds.fromString(runId);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Error parsing run-id.", e);
      }
      return runtimeInfos.get(run);
    }

    return findRuntimeInfo(programId);
  }

  @Nullable
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId) {
    Map<RunId, ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(programId.getType());
    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos.values()) {
      if (programId.equals(info.getProgramId().toEntityId())) {
        return info;
      }
    }
    return null;
  }

  /**
   * @see #setInstances(ProgramId, int, String)
   */
  public void setInstances(ProgramId programId, int instances) throws Exception {
    setInstances(programId, instances, null);
  }

  /**
   * Set instances for the given program. Only supported program types for this action are {@link ProgramType#FLOW},
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which instances are to be updated
   * @param instances the number of instances to be updated.
   * @param component the flowlet name. Only used when the program is a {@link ProgramType#FLOW flow}.
   * @throws InterruptedException if there is an error while asynchronously updating instances
   * @throws ExecutionException if there is an error while asynchronously updating instances
   * @throws BadRequestException if the number of instances specified is less than 0
   * @throws UnauthorizedException if the user does not have privileges to set instances for the specified program.
   *                               To set instances for a program, a user needs {@link Action#ADMIN} on the program.
   */
  public void setInstances(ProgramId programId, int instances, @Nullable String component) throws Exception {
    authorizerInstantiator.get().enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (instances < 1) {
      throw new BadRequestException(String.format("Instance count should be greater than 0. Got %s.", instances));
    }
    switch (programId.getType()) {
      case SERVICE:
        setServiceInstances(programId, instances);
        break;
      case WORKER:
        setWorkerInstances(programId, instances);
        break;
      case FLOW:
        setFlowletInstances(programId, component, instances);
        break;
      default:
        throw new BadRequestException(String.format("Setting instances for program type %s is not supported",
                                                    programId.getType().getPrettyName()));
    }
  }

  private void setWorkerInstances(ProgramId programId, int instances) throws ExecutionException, InterruptedException {
    int oldInstances = store.getWorkerInstances(programId.toId());
    if (oldInstances != instances) {
      store.setWorkerInstances(programId.toId(), instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of("runnable", programId.getProgram(),
                                                            "newInstances", String.valueOf(instances),
                                                            "oldInstances", String.valueOf(oldInstances))).get();
      }
    }
  }

  private void setFlowletInstances(ProgramId programId, String flowletId,
                                   int instances) throws ExecutionException, InterruptedException {
    int oldInstances = store.getFlowletInstances(programId.toId(), flowletId);
    if (oldInstances != instances) {
      FlowSpecification flowSpec = store.setFlowletInstances(programId.toId(), flowletId, instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
      if (runtimeInfo != null) {
        runtimeInfo.getController()
          .command(ProgramOptionConstants.INSTANCES,
                   ImmutableMap.of("flowlet", flowletId,
                                   "newInstances", String.valueOf(instances),
                                   "oldFlowSpec", GSON.toJson(flowSpec, FlowSpecification.class))).get();
      }
    }
  }

  private void setServiceInstances(ProgramId programId, int instances) throws ExecutionException, InterruptedException {
    int oldInstances = store.getServiceInstances(programId.toId());
    if (oldInstances != instances) {
      store.setServiceInstances(programId.toId(), instances);
      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.INSTANCES,
                                            ImmutableMap.of("runnable", programId.getProgram(),
                                                            "newInstances", String.valueOf(instances),
                                                            "oldInstances", String.valueOf(oldInstances))).get();
      }
    }
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
  @VisibleForTesting
  void validateAndCorrectRunningRunRecords(final ProgramType programType,
                                           final Set<String> processedInvalidRunRecordIds) {
    final Map<RunId, RuntimeInfo> runIdToRuntimeInfo = runtimeService.list(programType);

    LOG.trace("Start getting run records not actually running ...");
    List<RunRecordMeta> notActuallyRunning = store.getRuns(ProgramRunStatus.RUNNING,
                                                           new com.google.common.base.Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta input) {
        String runId = input.getPid();
        // Check if it is not actually running.
        return !runIdToRuntimeInfo.containsKey(RunIds.fromString(runId));
      }
    });
    LOG.trace("End getting {} run records not actually running.", notActuallyRunning.size());

    final Map<String, ProgramId> runIdToProgramId = new HashMap<>();

    LOG.trace("Start getting invalid run records  ...");
    Collection<RunRecordMeta> invalidRunRecords =
      Collections2.filter(notActuallyRunning, new com.google.common.base.Predicate<RunRecordMeta>() {
        @Override
        public boolean apply(RunRecordMeta input) {
          String runId = input.getPid();
          // check for program Id for the run record, if null then it is invalid program type.
          ProgramId targetProgramId = retrieveProgramIdForRunRecord(programType, runId);

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
      LOG.warn("Found {} RunRecords with RUNNING status but the program is not actually running for program type {}",
               invalidRunRecords.size(), programType.getPrettyName());
    } else {
      LOG.trace("No RunRecords found with RUNNING status but the program is not actually running for program type {}",
                programType.getPrettyName());
    }

    // Now lets correct the invalid RunRecords
    for (RunRecordMeta invalidRunRecordMeta : invalidRunRecords) {
      String runId = invalidRunRecordMeta.getPid();
      ProgramId targetProgramId = runIdToProgramId.get(runId);

      LOG.warn("Fixing RunRecord {} in program {} of type {} with RUNNING status but the program is not running",
               runId, targetProgramId, programType.getPrettyName());

      store.compareAndSetStatus(targetProgramId.toId(), runId, ProgramController.State.ALIVE.getRunStatus(),
                                ProgramController.State.ERROR.getRunStatus());

      processedInvalidRunRecordIds.add(runId);
    }
  }

  /**
   * Helper method to check if the run record is a child program of a Workflow
   *
   * @param runRecordMeta The target {@link RunRecordMeta} to check
   * @param processedInvalidRunRecordIds the {@link Set} of processed invalid run record ids.
   * @return {@code true} of we should check and {@code false} otherwise
   */
  private boolean shouldCorrectForWorkflowChildren(RunRecordMeta runRecordMeta,
                                                   Set<String> processedInvalidRunRecordIds) {
    // check if it is part of workflow because it may not have actual runtime info
    if (runRecordMeta.getProperties() != null && runRecordMeta.getProperties().get("workflowrunid") != null) {

      // Get the parent Workflow info
      String workflowRunId = runRecordMeta.getProperties().get("workflowrunid");
      if (!processedInvalidRunRecordIds.contains(workflowRunId)) {
        // If the parent workflow has not been processed, then check if it still valid
        ProgramId workflowProgramId = retrieveProgramIdForRunRecord(ProgramType.WORKFLOW, workflowRunId);
        if (workflowProgramId != null) {
          // lets see if the parent workflow run records state is still running
          RunRecordMeta wfRunRecord = store.getRun(workflowProgramId.toId(), workflowRunId);
          RuntimeInfo wfRuntimeInfo = runtimeService.lookup(workflowProgramId.toId(), RunIds.fromString(workflowRunId));

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
   * Helper method to get {@link ProgramId} for a RunRecord for type of program
   *
   * @param programType Type of program to search
   * @param runId The target id of the {@link RunRecord} to find
   * @return the program id of the run record or {@code null} if does not exist.
   */
  @Nullable
  private ProgramId retrieveProgramIdForRunRecord(ProgramType programType, String runId) {

    // Get list of namespaces (borrow logic from AbstractAppFabricHttpHandler#listPrograms)
    List<NamespaceMeta> namespaceMetas = nsStore.list();

    // For each, get all programs under it
    ProgramId targetProgramId = null;
    for (NamespaceMeta nm : namespaceMetas) {
      NamespaceId namespace = Ids.namespace(nm.getName());
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(namespace.toId());

      // For each application get the programs checked against run records
      for (ApplicationSpecification appSpec : appSpecs) {
        switch (programType) {
          case FLOW:
            for (String programName : appSpec.getFlows().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case MAPREDUCE:
            for (String programName : appSpec.getMapReduce().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case SPARK:
            for (String programName : appSpec.getSpark().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case SERVICE:
            for (String programName : appSpec.getServices().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case WORKER:
            for (String programName : appSpec.getWorkers().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case WORKFLOW:
            for (String programName : appSpec.getWorkflows().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case CUSTOM_ACTION:
          case WEBAPP:
            // no-op
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
   * Ensures that the logged-in user has a {@link Action privilege} on the specified program instance.
   *
   * @param programId the {@link ProgramId} to check for privileges
   * @throws UnauthorizedException if the logged in user has no {@link Action privileges} on the specified dataset
   */
  private void ensureAccess(ProgramId programId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!Principal.SYSTEM.equals(principal) && !filter.apply(programId)) {
      throw new UnauthorizedException(principal, Action.READ, programId);
    }
  }

  /**
   * Helper method to get program id for a run record if it exists in the store.
   *
   * @return instance of {@link ProgramId} if exist for the runId or null if does not
   */
  @Nullable
  private ProgramId validateProgramForRunRecord(String namespaceName, String appName, ProgramType programType,
                                                String programName, String runId) {
    ProgramId programId = Ids.namespace(namespaceName).app(appName).program(programType, programName);
    RunRecordMeta runRecord = store.getRun(programId.toId(), runId);
    if (runRecord == null) {
      return null;
    }
    return programId;
  }

  /**
   * Helper class to run in separate thread to validate the invalid running run records
   */
  private static class RunRecordsCorrectorRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RunRecordsCorrectorRunnable.class);

    private final ProgramLifecycleService programLifecycleService;

    RunRecordsCorrectorRunnable(ProgramLifecycleService programLifecycleService) {
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
        //noinspection ThrowableResultOfMethodCallIgnored
        LOG.warn("Unable to complete correcting run records: {}", Throwables.getRootCause(t).getMessage());
        LOG.debug("Exception thrown when running run id cleaner.", t);
      }
    }
  }
}
