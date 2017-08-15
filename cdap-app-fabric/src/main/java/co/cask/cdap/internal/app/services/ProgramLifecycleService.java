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

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.LogLevelUpdater;
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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.scheduler.Scheduler;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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

  private final Store store;
  private final ProgramRuntimeService runtimeService;
  private final NamespaceStore nsStore;
  private final PropertiesResolver propertiesResolver;
  private final PreferencesStore preferencesStore;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final Scheduler scheduler;

  @Inject
  ProgramLifecycleService(Store store, NamespaceStore nsStore, ProgramRuntimeService runtimeService,
                          PropertiesResolver propertiesResolver,
                          PreferencesStore preferencesStore, AuthorizationEnforcer authorizationEnforcer,
                          AuthenticationContext authenticationContext, Scheduler scheduler) {
    this.store = store;
    this.nsStore = nsStore;
    this.runtimeService = runtimeService;
    this.propertiesResolver = propertiesResolver;
    this.preferencesStore = preferencesStore;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.scheduler = scheduler;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ProgramLifecycleService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down ProgramLifecycleService");
  }

  /**
   * Returns the program status.
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  public ProgramStatus getProgramStatus(ProgramId programId) throws Exception {
    // check that app exists
    ApplicationId appId = programId.getParent();
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new NotFoundException(appId);
    }

    return getExistingAppProgramStatus(appSpec, programId);
  }

  /**
   * Returns the program status with no need of application existence check.
   * @param appSpec the ApplicationSpecification of the existing application
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  private ProgramStatus getExistingAppProgramStatus(ApplicationSpecification appSpec, ProgramId programId)
    throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());

    if (programId.getType() == ProgramType.WEBAPP) {
      throw new IllegalStateException("Webapp status is not supported");
    }

    ProgramSpecification spec = getExistingAppProgramSpecification(appSpec, programId);
    if (spec == null) {
      // program doesn't exist
      throw new NotFoundException(programId);
    }

    // A program is running if there are any RUNNING or STARTING run records
    boolean runningRunRecords = !store.getRuns(programId, ProgramRunStatus.RUNNING, 0, Long.MAX_VALUE, 1).isEmpty();
    if (runningRunRecords || !store.getRuns(programId, ProgramRunStatus.STARTING, 0, Long.MAX_VALUE, 1).isEmpty()) {
      return ProgramStatus.RUNNING;
    }
    return ProgramStatus.STOPPED;
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   *
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  public ProgramSpecification getProgramSpecification(ProgramId programId) throws Exception {
    AuthorizationUtil.ensureOnePrivilege(programId, EnumSet.of(Action.READ, Action.ADMIN), authorizationEnforcer,
                                         authenticationContext.getPrincipal());
    ApplicationSpecification appSpec;
    appSpec = store.getApplication(programId.getParent());
    if (appSpec == null) {
      return null;
    }
    return getExistingAppProgramSpecification(appSpec, programId);
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  private ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec, ProgramId programId)
    throws Exception {
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
   * @return {@link ProgramController}
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link Action#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public synchronized ProgramController start(ProgramId programId, Map<String, String> overrides, boolean debug)
    throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    if (isConcurrentRunsInSameAppForbidden(programId.getType()) && isRunningInSameProgram(programId)) {
      throw new ConflictException(String.format("Program %s is already running in an version of the same application",
                                                programId));
    }
    if (isRunning(programId) && !isConcurrentRunsAllowed(programId.getType())) {
      throw new ConflictException(String.format("Program %s is already running", programId));
    }

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(programId.toId());
    Map<String, String> userArgs = propertiesResolver.getUserProperties(programId.toId());
    if (overrides != null) {
      userArgs.putAll(overrides);
    }

    ProgramRuntimeService.RuntimeInfo runtimeInfo = startInternal(programId, sysArgs, userArgs, debug);
    if (runtimeInfo == null) {
      throw new IOException(String.format("Failed to start program %s", programId));
    }
    return runtimeInfo.getController();
  }

  /**
   * Start a Program.
   *
   * Note that this method can only be called through internal service, it does not have auth check for starting the
   * program.
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
  public ProgramRuntimeService.RuntimeInfo startInternal(final ProgramId programId,
                                                         final Map<String, String> systemArgs,
                                                         final Map<String, String> userArgs,
                                                         boolean debug) throws Exception {
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    BasicArguments systemArguments = new BasicArguments(systemArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.run(programDescriptor, new SimpleProgramOptions(
      programId, systemArguments, userArguments, debug));

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
   * @param runId the runId of the program run to stop. If null, all runs of the program as returned by
   *              {@link ProgramRuntimeService} are stopped.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws InterruptedException if there was a problem while waiting for the stop call to complete
   * @throws ExecutionException if there was a problem while waiting for the stop call to complete
   */
  public void stop(ProgramId programId, @Nullable String runId) throws Exception {
    List<ListenableFuture<ProgramController>> futures = issueStop(programId, runId);

    // Block until all stop requests completed. This call never throw ExecutionException
    Futures.successfulAsList(futures).get();

    Throwable failureCause = null;
    for (ListenableFuture<ProgramController> f : futures) {
      try {
        f.get();
      } catch (ExecutionException e) {
        // If the program is stopped in between the time listing runs and issuing stops of the program,
        // an IllegalStateException will be throw, which we can safely ignore
        if (!(e.getCause() instanceof IllegalStateException)) {
          if (failureCause == null) {
            failureCause = e.getCause();
          } else {
            failureCause.addSuppressed(e.getCause());
          }
        }
      }
    }
    if (failureCause != null) {
      throw new ExecutionException(String.format("%d out of %d runs of the program %s failed to stop",
                                                 failureCause.getSuppressed().length + 1, futures.size(), programId),
                                   failureCause);
    }
  }

  /**
   * Issues a command to stop the specified {@link RunId} of the specified {@link ProgramId} and returns a
   * {@link ListenableFuture} with the {@link ProgramController} for it.
   * Clients can wait for completion of the {@link ListenableFuture}.
   *
   * @param programId the {@link ProgramId program} to issue a stop for
   * @param runId the runId of the program run to stop. If null, all runs of the program as returned by
   *              {@link ProgramRuntimeService} are stopped.
   * @return a list of {@link ListenableFuture} with a {@link ProgramController} that clients can wait on for stop
   *         to complete.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws UnauthorizedException if the user issuing the command is not authorized to stop the program. To stop a
   *                               program, a user requires {@link Action#EXECUTE} permission on the program.
   */
  public List<ListenableFuture<ProgramController>> issueStop(ProgramId programId, @Nullable String runId)
    throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    List<ProgramRuntimeService.RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, runId);
    if (runtimeInfos.isEmpty()) {
      if (!store.applicationExists(programId.getParent())) {
        throw new ApplicationNotFoundException(programId.getParent());
      } else if (!store.programExists(programId)) {
        throw new ProgramNotFoundException(programId);
      } else if (runId != null) {
        ProgramRunId programRunId = programId.run(runId);
        // Check if the program is running and is started by the Workflow
        RunRecordMeta runRecord = store.getRun(programId, runId);
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
    List<ListenableFuture<ProgramController>> futures = new ArrayList<>();
    for (ProgramRuntimeService.RuntimeInfo runtimeInfo : runtimeInfos) {
      futures.add(runtimeInfo.getController().stop());
    }
    return futures;
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
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!store.programExists(programId)) {
      throw new NotFoundException(programId);
    }

    preferencesStore.setProperties(programId.getNamespace(), programId.getApplication(),
                                   programId.getType().getCategoryName(),
                                   programId.getProgram(), runtimeArgs);
  }

  /**
   * Gets runtime arguments for the program from the {@link PreferencesStore}
   *
   * @param programId the {@link ProgramId program} for which runtime arguments needs to be retrieved
   * @return {@link Map} containing runtime arguments of the program
   * @throws NotFoundException if the specified program was not found
   * @throws UnauthorizedException if the current user does not have sufficient privileges to get runtime arguments for
   * the specified program. To get runtime arguments for a program, a user requires
   * {@link Action#READ} privileges on the program.
   */
  public Map<String, String> getRuntimeArgs(@Name("programId") ProgramId programId) throws Exception {
    // user can have READ, ADMIN or EXECUTE to retrieve the runtime arguments
    AuthorizationUtil.ensureOnePrivilege(programId, EnumSet.of(Action.READ, Action.EXECUTE, Action.ADMIN),
                                         authorizationEnforcer, authenticationContext.getPrincipal());

    if (!store.programExists(programId)) {
      throw new NotFoundException(programId);
    }
    return preferencesStore.getProperties(programId.getNamespace(), programId.getApplication(),
                                          programId.getType().getCategoryName(), programId.getProgram());
  }

  /**
   * Update log levels for the given program. Only supported program types for this action are {@link ProgramType#FLOW},
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be updated
   * @param logLevels the {@link Map} of the log levels to be updated.
   * @param component the flowlet name. Only used when the program is a {@link ProgramType#FLOW flow}.
   * @param runId the run id of the program. {@code null} if update log levels for flowlet
   * @throws InterruptedException if there is an error while asynchronously updating log levels.
   * @throws ExecutionException if there is an error while asynchronously updating log levels.
   * @throws BadRequestException if the log level is not valid or the program type is not supported.
   * @throws UnauthorizedException if the user does not have privileges to update log levels for the specified program.
   *                               To update log levels for a program, a user needs {@link Action#ADMIN} on the program.
   */
  public void updateProgramLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                                     @Nullable String component, @Nullable String runId) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(String.format("Updating log levels for program type %s is not supported",
                                                  programId.getType().getPrettyName()));
    }
    updateLogLevels(programId, logLevels, component, runId);
  }

  /**
   * Reset log levels for the given program. Only supported program types for this action are {@link ProgramType#FLOW},
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be reset.
   * @param loggerNames the {@link String} set of the logger names to be updated, empty means reset for all
   *                    loggers.
   * @param component the flowlet name. Only used when the program is a {@link ProgramType#FLOW flow}.
   * @param runId the run id of the program. {@code null} if set log levels for flowlet
   * @throws InterruptedException if there is an error while asynchronously resetting log levels.
   * @throws ExecutionException if there is an error while asynchronously resetting log levels.
   * @throws UnauthorizedException if the user does not have privileges to reset log levels for the specified program.
   *                               To reset log levels for a program, a user needs {@link Action#ADMIN} on the program.
   */
  public void resetProgramLogLevels(ProgramId programId, Set<String> loggerNames,
                                    @Nullable String component, @Nullable String runId) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
    if (!EnumSet.of(ProgramType.FLOW, ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(String.format("Resetting log levels for program type %s is not supported",
                                                  programId.getType().getPrettyName()));
    }
    resetLogLevels(programId, loggerNames, component, runId);
  }

  public boolean programExists(ProgramId programId) throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());
    return store.programExists(programId);
  }

  private boolean isRunning(ProgramId programId) throws Exception {
    return ProgramStatus.STOPPED != getProgramStatus(programId);
  }

  /**
   * Returns whether the given program is running in any versions of the app.
   * @param programId the id of the program for which the running status in all versions of the app is found
   * @return whether the given program is running in any versions of the app
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  private boolean isRunningInSameProgram(ProgramId programId) throws Exception {
    // check that app exists
    Collection<ApplicationId> appIds = store.getAllAppVersionsAppIds(programId.getParent());
    if (appIds == null) {
      throw new NotFoundException(Id.Application.from(programId.getNamespace(), programId.getApplication()));
    }
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    for (ApplicationId appId : appIds) {
      ProgramId pId = appId.program(programId.getType(), programId.getProgram());
      if (getExistingAppProgramStatus(appSpec, pId).equals(ProgramStatus.RUNNING)) {
        return true;
      }
    }
    return false;
  }

  private boolean isConcurrentRunsInSameAppForbidden(ProgramType type) {
    // Concurrent runs in different (or same) versions of an application are forbidden for worker and flow
    return EnumSet.of(ProgramType.WORKER, ProgramType.FLOW).contains(type);
  }

  private boolean isConcurrentRunsAllowed(ProgramType type) {
    // Concurrent runs are only allowed for the Workflow, MapReduce and Spark
    return EnumSet.of(ProgramType.WORKFLOW, ProgramType.MAPREDUCE, ProgramType.SPARK).contains(type);
  }

  private List<ProgramRuntimeService.RuntimeInfo> findRuntimeInfo(ProgramId programId,
                                                                  @Nullable String runId) throws BadRequestException {
    if (runId != null) {
      RunId run;
      try {
        run = RunIds.fromString(runId);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Error parsing run-id.", e);
      }
      ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.lookup(programId, run);
      return runtimeInfo == null ? Collections.<RuntimeInfo>emptyList() : Collections.singletonList(runtimeInfo);
    }
    return new ArrayList<>(runtimeService.list(programId).values());
  }

  @Nullable
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId) throws BadRequestException {
    List<ProgramRuntimeService.RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, null);
    return runtimeInfos.isEmpty() ? null : runtimeInfos.iterator().next();
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
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.ADMIN);
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

  /**
   * Gets the state of the given schedule
   *
   * @return the status of the given schedule
   * @throws Exception if failed to get the state of the schedule
   */
  public ProgramScheduleStatus getScheduleStatus(ScheduleId scheduleId)
    throws Exception {
    ApplicationId applicationId = scheduleId.getParent();
    ApplicationSpecification appSpec = store.getApplication(applicationId);
    if (appSpec == null) {
      throw new NotFoundException(applicationId);
    }
    ProgramSchedule schedule = scheduler.getSchedule(scheduleId);
    AuthorizationUtil.ensureAccess(schedule.getProgramId(), authorizationEnforcer,
                                   authenticationContext.getPrincipal());
    return scheduler.getScheduleStatus(scheduleId);
  }

  /**
   * Performs an action (suspend/resume) on the given schedule
   *
   * @param scheduleId Id of the schedule
   * @param action the action to perform
   * @throws Exception if the given action is invalid or failed to perform a valid action on the schedule
   */
  public void suspendResumeSchedule(ScheduleId scheduleId, String action) throws Exception {
    boolean doEnable;
    if (action.equals("disable") || action.equals("suspend")) {
      doEnable = false;
    } else if (action.equals("enable") || action.equals("resume")) {
      doEnable = true;
    } else {
      throw new BadRequestException(
        "Action for schedules may only be 'enable', 'disable', 'suspend', or 'resume' but is'" + action + "'");
    }
    ProgramSchedule schedule = scheduler.getSchedule(scheduleId);
    authorizationEnforcer.enforce(schedule.getProgramId(), authenticationContext.getPrincipal(), Action.EXECUTE);
    if (doEnable) {
      scheduler.enableSchedule(scheduleId);
    } else {
      scheduler.disableSchedule(scheduleId);
    }
  }

  /**
   * Lists all programs with the specified program type in a namespace. If perimeter security and authorization are
   * enabled, only returns the programs that the current user has access to.
   *
   * @param namespaceId the namespace to list datasets for
   * @return the programs in the provided namespace
   */
  public List<ProgramRecord> list(NamespaceId namespaceId, ProgramType type) throws Exception {
    Collection<ApplicationSpecification> appSpecs = store.getAllApplications(namespaceId);
    List<ProgramRecord> programRecords = new ArrayList<>();
    for (ApplicationSpecification appSpec : appSpecs) {
      switch (type) {
        case FLOW:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getFlows().values(), programRecords);
          break;
        case MAPREDUCE:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getMapReduce().values(), programRecords);
          break;
        case SPARK:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getSpark().values(), programRecords);
          break;
        case SERVICE:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getServices().values(), programRecords);
          break;
        case WORKER:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getWorkers().values(), programRecords);
          break;
        case WORKFLOW:
          createProgramRecords(namespaceId, appSpec.getName(), type, appSpec.getWorkflows().values(), programRecords);
          break;
        default:
          throw new Exception("Unknown program type: " + type.name());
      }
    }
    return programRecords;
  }

  private void createProgramRecords(NamespaceId namespaceId, String appId, ProgramType type,
                                    Iterable<? extends ProgramSpecification> programSpecs,
                                    List<ProgramRecord> programRecords) throws Exception {
    for (ProgramSpecification programSpec : programSpecs) {
      if (hasAccess(namespaceId.app(appId).program(type, programSpec.getName()))) {
        programRecords.add(new ProgramRecord(type, appId, programSpec.getName(), programSpec.getDescription()));
      }
    }
  }

  private boolean hasAccess(ProgramId programId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    return !authorizationEnforcer.isVisible(Collections.singleton(programId), principal).isEmpty();
  }

  private void setWorkerInstances(ProgramId programId, int instances)
    throws ExecutionException, InterruptedException, BadRequestException {
    int oldInstances = store.getWorkerInstances(programId);
    if (oldInstances != instances) {
      store.setWorkerInstances(programId, instances);
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
                                   int instances) throws ExecutionException, InterruptedException, BadRequestException {
    int oldInstances = store.getFlowletInstances(programId, flowletId);
    if (oldInstances != instances) {
      FlowSpecification flowSpec = store.setFlowletInstances(programId, flowletId, instances);
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

  private void setServiceInstances(ProgramId programId, int instances)
    throws ExecutionException, InterruptedException, BadRequestException {
    int oldInstances = store.getServiceInstances(programId);
    if (oldInstances != instances) {
      store.setServiceInstances(programId, instances);
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
   * Helper method to get {@link ProgramId} for a RunRecord for type of program
   *
   * @param programType Type of program to search
   * @param runId The target id of the {@link RunRecord} to find
   * @return the program id of the run record or {@code null} if does not exist.
   */
  @Nullable
  protected ProgramId retrieveProgramIdForRunRecord(ProgramType programType, String runId) {

    // Get list of namespaces (borrow logic from AbstractAppFabricHttpHandler#listPrograms)
    List<NamespaceMeta> namespaceMetas = nsStore.list();

    // For each, get all programs under it
    ProgramId targetProgramId = null;
    for (NamespaceMeta nm : namespaceMetas) {
      NamespaceId namespace = Ids.namespace(nm.getName());
      Collection<ApplicationSpecification> appSpecs = store.getAllApplications(namespace);

      // For each application get the programs checked against run records
      for (ApplicationSpecification appSpec : appSpecs) {
        switch (programType) {
          case FLOW:
            for (String programName : appSpec.getFlows().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(),
                                                                appSpec.getAppVersion(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case MAPREDUCE:
            for (String programName : appSpec.getMapReduce().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(),
                                                                appSpec.getAppVersion(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case SPARK:
            for (String programName : appSpec.getSpark().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(),
                                                                appSpec.getAppVersion(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case SERVICE:
            for (String programName : appSpec.getServices().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(),
                                                                appSpec.getAppVersion(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case WORKER:
            for (String programName : appSpec.getWorkers().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(),
                                                                appSpec.getAppVersion(), programType,
                                                                programName, runId);
              if (programId != null) {
                targetProgramId = programId;
                break;
              }
            }
            break;
          case WORKFLOW:
            for (String programName : appSpec.getWorkflows().keySet()) {
              ProgramId programId = validateProgramForRunRecord(nm.getName(), appSpec.getName(),
                                                                appSpec.getAppVersion(), programType,
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
   * Helper method to get program id for a run record if it exists in the store.
   *
   * @return instance of {@link ProgramId} if exist for the runId or null if does not
   */
  @Nullable
  private ProgramId validateProgramForRunRecord(String namespaceName, String appName, String appVersion,
                                                ProgramType programType, String programName, String runId) {
    ProgramId programId = Ids.namespace(namespaceName).app(appName, appVersion).program(programType, programName);
    RunRecordMeta runRecord = store.getRun(programId, runId);
    if (runRecord == null) {
      return null;
    }
    return programId;
  }

  /**
   * Helper method to update log levels for Worker, Flow, or Service.
   */
  private void updateLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                               @Nullable String component, @Nullable String runId) throws Exception {
    List<ProgramRuntimeService.RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, runId);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeInfos.isEmpty() ? null : runtimeInfos.get(0);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.updateLogLevels(logLevels, component);
    }
  }

  /**
   * Helper method to reset log levels for Worker, Flow or Service.
   */
  private void resetLogLevels(ProgramId programId, Set<String> loggerNames,
                              @Nullable String component, @Nullable String runId) throws Exception {
    List<ProgramRuntimeService.RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, runId);
    ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeInfos.isEmpty() ? null : runtimeInfos.get(0);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.resetLogLevels(loggerNames, component);
    }
  }

  /**
   * Helper method to get the {@link LogLevelUpdater} for the program.
   */
  private LogLevelUpdater getLogLevelUpdater(RuntimeInfo runtimeInfo) throws Exception {
    ProgramController programController = runtimeInfo.getController();
    if (!(programController instanceof LogLevelUpdater)) {
      throw new BadRequestException("Update log levels at runtime is only supported in distributed mode");
    }
    return ((LogLevelUpdater) programController);
  }
}
