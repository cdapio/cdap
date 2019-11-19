/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.LogLevelUpdater;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProfileConflictException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.config.PreferencesService;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.pipeline.PluginRequirement;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.internal.provision.ProvisionerNotifier;
import co.cask.cdap.internal.provision.ProvisioningOp;
import co.cask.cdap.internal.provision.ProvisioningService;
import co.cask.cdap.internal.provision.ProvisioningTaskInfo;
import co.cask.cdap.proto.ProgramHistory;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunCountResult;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.runtime.spi.profile.ProfileStatus;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.logging.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.security.auth.kerberos.KerberosPrincipal;

/**
 * Service that manages lifecycle of Programs.
 */
public class ProgramLifecycleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleService.class);

  private static final Gson GSON = ApplicationSpecificationAdapter
    .addTypeAdapters(new GsonBuilder())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  private final Store store;
  private final ProfileService profileService;
  private final ProgramRuntimeService runtimeService;
  private final PropertiesResolver propertiesResolver;
  private final PreferencesService preferencesService;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private static final String RUNTIME_ARG_KEYTAB = "pipeline.keytab.path";
  private static final String RUNTIME_ARG_PRINCIPAL = "pipeline.principal.name";

  @Inject
  ProgramLifecycleService(Store store, ProfileService profileService, ProgramRuntimeService runtimeService,
                          PropertiesResolver propertiesResolver,
                          PreferencesService preferencesService, AuthorizationEnforcer authorizationEnforcer,
                          AuthenticationContext authenticationContext,
                          ProvisionerNotifier provisionerNotifier, ProvisioningService provisioningService,
                          ProgramStateWriter programStateWriter) {
    this.store = store;
    this.profileService = profileService;
    this.runtimeService = runtimeService;
    this.propertiesResolver = propertiesResolver;
    this.preferencesService = preferencesService;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.provisionerNotifier = provisionerNotifier;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
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
   * Returns the program run count of the given program.
   *
   * @param programId the id of the program for which the count call is made
   * @return the run count of the program
   * @throws NotFoundException if the application to which this program belongs was not found or the program is not
   *                           found in the app
   */
  public long getProgramRunCount(ProgramId programId) throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());
    return store.getProgramRunCount(programId);
  }

  /**
   * Returns the program run count of the given program id list.
   *
   * @param programIds the list of program ids to get the count
   * @return the counts of given program ids
   */
  public List<RunCountResult> getProgramRunCounts(List<ProgramId> programIds) throws Exception {
    List<RunCountResult> result = store.getProgramRunCounts(programIds);

    // filter the result
    Set<? extends EntityId> visibleEntities = authorizationEnforcer.isVisible(new HashSet<>(programIds),
                                                                              authenticationContext.getPrincipal());
    return result.stream()
      .map(runCount -> {
        if (!visibleEntities.contains(runCount.getProgramId())) {
          return new RunCountResult(runCount.getProgramId(), null,
                                    new UnauthorizedException(authenticationContext.getPrincipal(),
                                                              runCount.getProgramId()));
        }
        return runCount;
      })
      .collect(Collectors.toList());
  }

  /**
   * Get the latest runs within the specified start and end times for the specified program.
   *
   * @param programId the program to get runs for
   * @param programRunStatus status of runs to return
   * @param start earliest start time of runs to return
   * @param end latest start time of runs to return
   * @param limit the maximum number of runs to return
   * @return the latest runs for the program sorted by start time, with the newest run as the first run
   * @throws NotFoundException if the application to which this program belongs was not found or the program is not
   *                           found in the app
   * @throws UnauthorizedException if the principal does not have access to the program
   * @throws Exception if there was some other exception performing authorization checks
   */
  public List<RunRecord> getRuns(ProgramId programId, ProgramRunStatus programRunStatus,
                                 long start, long end, int limit) throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());
    ProgramSpecification programSpec = getProgramSpecificationWithoutAuthz(programId);
    if (programSpec == null) {
      throw new NotFoundException(programId);
    }
    return store.getRuns(programId, programRunStatus, start, end, limit).values().stream()
      .map(record -> RunRecord.builder(record).build()).collect(Collectors.toList());
  }

  /**
   * Get the latest runs within the specified start and end times for the specified programs.
   *
   * @param programs the programs to get runs for
   * @param programRunStatus status of runs to return
   * @param start earliest start time of runs to return
   * @param end latest start time of runs to return
   * @param limit the maximum number of runs to return
   * @return the latest runs for the program sorted by start time, with the newest run as the first run
   * @throws NotFoundException if the application to which this program belongs was not found or the program is not
   *                           found in the app
   * @throws UnauthorizedException if the principal does not have access to the program
   * @throws Exception if there was some other exception performing authorization checks
   */
  public List<ProgramHistory> getRuns(Collection<ProgramId> programs, ProgramRunStatus programRunStatus,
                                      long start, long end, int limit) throws Exception {
    List<ProgramHistory> result = new ArrayList<>();

    // do this in batches to avoid transaction timeouts.
    List<ProgramId> batch = new ArrayList<>(20);

    for (ProgramId program : programs) {
      batch.add(program);

      if (batch.size() >= 20) {
        addProgramHistory(result, batch, programRunStatus, start, end, limit);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      addProgramHistory(result, batch, programRunStatus, start, end, limit);
    }
    return result;
  }

  private void addProgramHistory(List<ProgramHistory> histories, List<ProgramId> programs,
                                 ProgramRunStatus programRunStatus, long start, long end, int limit) throws Exception {
    Set<? extends EntityId> visibleEntities = authorizationEnforcer.isVisible(new HashSet<>(programs),
                                                                              authenticationContext.getPrincipal());
    for (ProgramHistory programHistory : store.getRuns(programs, programRunStatus, start, end, limit, x -> true)) {
      ProgramId programId = programHistory.getProgramId();
      if (visibleEntities.contains(programId)) {
        histories.add(programHistory);
      } else {
        histories.add(new ProgramHistory(programId, Collections.emptyList(),
                                      new UnauthorizedException(authenticationContext.getPrincipal(), programId)));
      }
    }
  }

  /**
   * Returns the program status with no need of application existence check.
   * @param appSpec the ApplicationSpecification of the existing application
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  private ProgramStatus getExistingAppProgramStatus(ApplicationSpecification appSpec,
                                                    ProgramId programId) throws Exception {
    AuthorizationUtil.ensureAccess(programId, authorizationEnforcer, authenticationContext.getPrincipal());
    ProgramSpecification spec = getExistingAppProgramSpecification(appSpec, programId);
    if (spec == null) {
      // program doesn't exist
      throw new NotFoundException(programId);
    }

    return getProgramStatus(store.getActiveRuns(programId).values());
  }

  /**
   * Returns the program status based on the active run records of a program.
   * A program is RUNNING if there are any RUNNING or SUSPENDED run records.
   * A program is starting if there are any PENDING or STARTING run records and no RUNNING run records.
   * Otherwise, it is STOPPED.
   *
   * @param runRecords run records for the program
   * @return the program status
   */
  @VisibleForTesting
  static ProgramStatus getProgramStatus(Collection<RunRecordMeta> runRecords) {
    boolean hasStarting = false;
    for (RunRecordMeta runRecord : runRecords) {
      ProgramRunStatus runStatus = runRecord.getStatus();
      if (runStatus == ProgramRunStatus.RUNNING || runStatus == ProgramRunStatus.SUSPENDED) {
        return ProgramStatus.RUNNING;
      }
      hasStarting = hasStarting || runStatus == ProgramRunStatus.STARTING || runStatus == ProgramRunStatus.PENDING;
    }
    return hasStarting ? ProgramStatus.STARTING : ProgramStatus.STOPPED;
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   *
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  public ProgramSpecification getProgramSpecification(ProgramId programId) throws Exception {
    AuthorizationUtil.ensureOnePrivilege(programId, EnumSet.allOf(Action.class), authorizationEnforcer,
                                         authenticationContext.getPrincipal());
    return getProgramSpecificationWithoutAuthz(programId);
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  private ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec,
                                                                  ProgramId programId) {
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
   * @return {@link RunId}
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link Action#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public synchronized RunId run(ProgramId programId, Map<String, String> overrides, boolean debug) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);
    if (isConcurrentRunsInSameAppForbidden(programId.getType()) && !isStoppedInSameProgram(programId)) {
      throw new ConflictException(String.format("Program %s is already running in an version of the same application",
                                                programId));
    }
    if (!isStopped(programId) && !isConcurrentRunsAllowed(programId.getType())) {
      throw new ConflictException(String.format("Program %s is already running", programId));
    }

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(Id.Program.fromEntityId(programId));
    if (checkUserIdentityPropagationPreference(programId)) {
        LOG.debug("user identity propagation is enabled");
        sysArgs.put(ProgramOptionConstants.LOGGED_IN_USER, authenticationContext.getPrincipal().getName());
    }
    
    Map<String, String> userArgs = propertiesResolver.getUserProperties(Id.Program.fromEntityId(programId));
    
    if (overrides != null) {
      userArgs.putAll(overrides);
    }
    
    if ((userArgs.containsKey(RUNTIME_ARG_KEYTAB)) && 
            (userArgs.containsKey(RUNTIME_ARG_PRINCIPAL))) {
        String principal = userArgs.get(RUNTIME_ARG_PRINCIPAL);
        LOG.debug("Checking authorisation for user: " + authenticationContext.getPrincipal() +
                " , using runtime config principal: " + principal);
        KerberosPrincipalId kid = new KerberosPrincipalId(principal);
        authorizationEnforcer.enforce(kid, authenticationContext.getPrincipal(), Action.ADMIN);
    }
    return runInternal(programId, userArgs, sysArgs, debug);
  }


  /**
   * Runs a Program without authorization.
   *
   * Note that this method should only be called through internal service, it does not have auth check for starting the
   * program.
   *
   * @param programId the {@link ProgramId program} to run
   * @param userArgs user arguments
   * @param sysArgs system arguments
   * @param debug whether to start as a debug run
   * @return {@link RunId}
   * @throws IOException if there is an error starting the program
   * @throws NotFoundException if the namespace, application, or program is not found
   * @throws ProfileConflictException if the profile is disabled
   */
  public synchronized RunId runInternal(ProgramId programId, Map<String, String> userArgs,
                                        Map<String, String> sysArgs,
                                        boolean debug) throws NotFoundException, IOException, ProfileConflictException {
    LOG.info("Attempt to run {} program {} as user {}", programId.getType(), programId.getProgram(),
             authenticationContext.getPrincipal().getName());

    ProgramOptions programOptions = createProgramOptions(programId, userArgs, sysArgs, debug);

    RunId runId = RunIds.generate();
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    String userId = SecurityRequestContext.getUserId();
    userId = userId == null ? "" : userId;
    provisionerNotifier.provisioning(programId.run(runId), programOptions, programDescriptor, userId);
    return runId;
  }

  @VisibleForTesting
  ProgramOptions createProgramOptions(ProgramId programId, Map<String, String> userArgs, Map<String, String> sysArgs,
                                      boolean debug) throws NotFoundException, ProfileConflictException {
    ProfileId profileId = SystemArguments.getProfileIdForProgram(programId, userArgs);
    Map<String, String> profileProperties = SystemArguments.getProfileProperties(userArgs);
    Profile profile = profileService.getProfile(profileId, profileProperties);
    if (profile.getStatus() == ProfileStatus.DISABLED) {
      throw new ProfileConflictException(String.format("Profile %s in namespace %s is disabled. It cannot be " +
                                                         "used to start the program %s",
                                                       profileId.getProfile(), profileId.getNamespace(),
                                                       programId.toString()), profileId);
    }
    ProvisionerDetail spec = provisioningService.getProvisionerDetail(profile.getProvisioner().getName());
    if (spec == null) {
      throw new NotFoundException(String.format("Provisioner '%s' not found.", profile.getProvisioner().getName()));
    }
    // get and add any user overrides for profile properties
    Map<String, String> systemArgs = new HashMap<>(sysArgs);
    // add profile properties to the system arguments
    SystemArguments.addProfileArgs(systemArgs, profile);

    // Set the ClusterMode. If it is NATIVE profile, then it is ON_PREMISE, otherwise is ISOLATED
    // This should probably move into the provisioner later once we have a better contract for the
    // provisioner to actually pick what launching mechanism it wants to use.
    systemArgs.put(ProgramOptionConstants.CLUSTER_MODE,
                   (ProfileId.NATIVE.equals(profileId) ? ClusterMode.ON_PREMISE : ClusterMode.ISOLATED).name());
    ProgramSpecification programSpecification = getProgramSpecificationWithoutAuthz(programId);
    if (programSpecification == null) {
      throw new NotFoundException(programId);
    }
    // put all the plugin requirements if any involved in the run
    systemArgs.put(ProgramOptionConstants.PLUGIN_REQUIREMENTS,
                   GSON.toJson(getPluginRequirements(programSpecification)));
    return new SimpleProgramOptions(programId, new BasicArguments(systemArgs), new BasicArguments(userArgs), debug);
  }

  /**
   * Starts a Program with the specified argument overrides, skipping cluster lifecycle steps in the run.
   * NOTE: This method should only be called from preview runner.
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
    if (isConcurrentRunsInSameAppForbidden(programId.getType()) && !isStoppedInSameProgram(programId)) {
      throw new ConflictException(String.format("Program %s is already running in an version of the same application",
                                                programId));
    }
    if (!isStopped(programId) && !isConcurrentRunsAllowed(programId.getType())) {
      throw new ConflictException(String.format("Program %s is already running", programId));
    }

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(Id.Program.fromEntityId(programId));
    sysArgs.put(ProgramOptionConstants.SKIP_PROVISIONING, "true");
    sysArgs.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    if (checkUserIdentityPropagationPreference(programId)) {
        sysArgs.put(ProgramOptionConstants.LOGGED_IN_USER, authenticationContext.getPrincipal().getName());
    }
    
    Map<String, String> userArgs = propertiesResolver.getUserProperties(Id.Program.fromEntityId(programId));
    if (overrides != null) {
      userArgs.putAll(overrides);
    }

    if ((userArgs.containsKey(RUNTIME_ARG_KEYTAB)) && 
            (userArgs.containsKey(RUNTIME_ARG_PRINCIPAL))) {
        String principal = userArgs.get(RUNTIME_ARG_PRINCIPAL);
        LOG.debug("Checking authorisation for user: " + authenticationContext.getPrincipal() +
                " , using runtime config principal: " + principal);
        KerberosPrincipalId kid = new KerberosPrincipalId(principal);
        authorizationEnforcer.enforce(kid, authenticationContext.getPrincipal(), Action.ADMIN);
    }
    
    BasicArguments systemArguments = new BasicArguments(sysArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramOptions options = new SimpleProgramOptions(programId, systemArguments, userArguments, debug);
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    ProgramRunId programRunId = programId.run(RunIds.generate());

    programStateWriter.start(programRunId, options, null, programDescriptor);
    return startInternal(programDescriptor, options, programRunId);
  }

  /**
   * Starts a Program with the specified argument overrides. Does not perform authorization checks, and is meant to
   * only be used by internal services. If the program is already started, returns the controller for the program.
   *
   * @param programDescriptor descriptor of the program to run
   * @param programOptions options for the program run
   * @param programRunId program run id
   * @return controller for the program
   * @throws IOException if there was a failure starting the program
   */
  synchronized ProgramController startInternal(ProgramDescriptor programDescriptor,
                                               ProgramOptions programOptions,
                                               ProgramRunId programRunId) throws IOException {
    RunId runId = RunIds.fromString(programRunId.getRun());
    RuntimeInfo runtimeInfo = runtimeService.lookup(programRunId.getParent(), runId);
    if (runtimeInfo != null) {
      return runtimeInfo.getController();
    }

    runtimeInfo = runtimeService.run(programDescriptor, programOptions, runId);
    if (runtimeInfo == null) {
      throw new IOException(String.format("Failed to start program %s", programRunId));
    }
    return runtimeInfo.getController();
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
    List<ListenableFuture<ProgramRunId>> futures = issueStop(programId, runId);

    // Block until all stop requests completed. This call never throw ExecutionException
    Futures.successfulAsList(futures).get();

    Throwable failureCause = null;
    for (ListenableFuture<ProgramRunId> f : futures) {
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
   * {@link ListenableFuture} with the {@link ProgramRunId} for the runs that were stopped.
   * Clients can wait for completion of the {@link ListenableFuture}.
   *
   * @param programId the {@link ProgramId program} to issue a stop for
   * @param runId the runId of the program run to stop. If null, all runs of the program as returned by
   *              {@link ProgramRuntimeService} are stopped.
   * @return a list of {@link ListenableFuture} with the {@link ProgramRunId} that clients can wait on for stop
   *         to complete.
   * @throws NotFoundException if the app, program or run was not found
   * @throws BadRequestException if an attempt is made to stop a program that is either not running or
   *                             was started by a workflow
   * @throws UnauthorizedException if the user issuing the command is not authorized to stop the program. To stop a
   *                               program, a user requires {@link Action#EXECUTE} permission on the program.
   */
  public List<ListenableFuture<ProgramRunId>> issueStop(ProgramId programId, @Nullable String runId) throws Exception {
    authorizationEnforcer.enforce(programId, authenticationContext.getPrincipal(), Action.EXECUTE);

    // See if the program is running as per the runtime service
    Map<RunId, RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, runId);
    Map<ProgramRunId, RunRecordMeta> activeRunRecords = getActiveRuns(programId, runId);

    if (runtimeInfos.isEmpty() && activeRunRecords.isEmpty()) {
      // Error out if no run information from runtime service and from run record
      if (!store.applicationExists(programId.getParent())) {
        throw new ApplicationNotFoundException(programId.getParent());
      } else if (!store.programExists(programId)) {
        throw new ProgramNotFoundException(programId);
      }
      throw new BadRequestException(String.format("Program '%s' is not running.", programId));
    }

    // Stop the running program based on a combination of runtime info and run record
    // It's possible that some of them are not yet available from the runtimeService due to timing
    // differences between the run record was created vs being added to runtimeService
    // So we retry in a loop for up to 3 seconds max to cater for those cases

    Set<String> pendingStops = Stream.concat(runtimeInfos.keySet().stream().map(RunId::getId),
                                             activeRunRecords.keySet().stream().map(ProgramRunId::getRun))
                                      .collect(Collectors.toSet());

    List<ListenableFuture<ProgramRunId>> futures = new ArrayList<>();
    Stopwatch stopwatch = new Stopwatch().start();

    Set<ProgramRunId> cancelledProvisionRuns = new HashSet<>();
    while (!pendingStops.isEmpty() && stopwatch.elapsedTime(TimeUnit.SECONDS) < 3L) {
      Iterator<String> iterator = pendingStops.iterator();
      while (iterator.hasNext()) {
        ProgramRunId activeRunId = programId.run(iterator.next());
        RunRecordMeta runRecord = activeRunRecords.get(activeRunId);
        if (runRecord == null) {
          runRecord = store.getRun(activeRunId);
        }
        // Check if the program is actually started from workflow and the workflow is running
        if (runRecord != null && runRecord.getProperties().containsKey("workflowrunid")
          && runRecord.getStatus().equals(ProgramRunStatus.RUNNING)) {
          String workflowRunId = runRecord.getProperties().get("workflowrunid");
          throw new BadRequestException(String.format("Cannot stop the program '%s' started by the Workflow " +
                                                        "run '%s'. Please stop the Workflow.", activeRunId,
                                                      workflowRunId));
        }

        RuntimeInfo runtimeInfo = runtimeService.lookup(programId, RunIds.fromString(activeRunId.getRun()));
        // if there is a runtimeInfo, the run is in the 'starting' state or later
        if (runtimeInfo != null) {
          ListenableFuture<ProgramController> future = runtimeInfo.getController().stop();
          futures.add(Futures.transform(future, ProgramController::getProgramRunId));
          iterator.remove();
          // if it was in this set, it means we cancelled a task, but it had already sent a PROVISIONED message
          // by the time we cancelled it. We then waited for it to show up in the runtime service and got here.
          // We added a future for this run in the lines above, but we don't want to add another duplicate future
          // at the end of this loop, so remove this run from the cancelled provision runs.
          cancelledProvisionRuns.remove(activeRunId);
        } else {
          // if there is no runtimeInfo, the run could be in the provisioning state.
          Optional<ProvisioningTaskInfo> cancelledInfo = provisioningService.cancelProvisionTask(activeRunId);
          cancelledInfo.ifPresent(taskInfo -> {
            cancelledProvisionRuns.add(activeRunId);
            // This state check is to handle a race condition where we cancel the provision task, but not in time
            // to prevent it from sending the PROVISIONED notification.

            // If the notification was sent, but not yet consumed, we are *not* done stopping the run.
            // We have to wait for the notification to be consumed, which will start the run, and place the controller
            // in the runtimeService. The next time we loop, we can find it in the runtimeService and tell it to stop.
            // If the notification was not sent, then we *are* done stopping the run.

            // Therefore, if the state is CREATED, we don't remove it from the iterator so that the run will get
            // checked again in the next loop, when we may get the controller from the runtimeService to stop it.

            // No other task states have this race condition, as the PROVISIONED notification is only sent
            // after the state transitions to CREATED. Therefore it is safe to remove the runId from the iterator,
            // as we know we are done stopping it.
            ProvisioningOp.Status taskState = taskInfo.getProvisioningOp().getStatus();
            if (taskState != ProvisioningOp.Status.CREATED) {
              iterator.remove();
            }
          });
        }
      }

      if (!pendingStops.isEmpty()) {
        // If not able to stop all of them, it means there were some runs that didn't have a runtime info and
        // didn't have a provisioning task. This can happen if the run was already finished, or the run transitioned
        // from the provisioning state to the starting state during this stop operation.
        // We'll get the active runs again and filter it by the pending stops. Stop will be retried for those.
        Set<String> finalPendingStops = pendingStops;

        activeRunRecords = getActiveRuns(programId, runId).entrySet().stream()
          .filter(e -> finalPendingStops.contains(e.getKey().getRun()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        pendingStops = activeRunRecords.keySet().stream().map(ProgramRunId::getRun).collect(Collectors.toSet());

        if (!pendingStops.isEmpty()) {
          TimeUnit.MILLISECONDS.sleep(200);
        }
      }
    }

    for (ProgramRunId cancelledProvisionRun : cancelledProvisionRuns) {
      SettableFuture<ProgramRunId> future = SettableFuture.create();
      future.set(cancelledProvisionRun);
      futures.add(future);
    }
    return futures;
  }

  /**
   * Save runtime arguments for all future runs of this program. The runtime arguments are saved in the
   * {@link PreferencesService}.
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

    preferencesService.setProperties(programId, runtimeArgs);
  }

  /**
   * Gets runtime arguments for the program from the {@link PreferencesService}
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
    return preferencesService.getProperties(programId);
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

  private boolean isStopped(ProgramId programId) throws Exception {
    return ProgramStatus.STOPPED == getProgramStatus(programId);
  }

  /**
   * Returns whether the given program is stopped in all versions of the app.
   * @param programId the id of the program for which the stopped status in all versions of the app is found
   * @return whether the given program is stopped in all versions of the app
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  private boolean isStoppedInSameProgram(ProgramId programId) throws Exception {
    // check that app exists
    Collection<ApplicationId> appIds = store.getAllAppVersionsAppIds(programId.getParent());
    if (appIds == null || appIds.isEmpty()) {
      throw new NotFoundException(Id.Application.from(programId.getNamespace(), programId.getApplication()));
    }
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    for (ApplicationId appId : appIds) {
      ProgramId pId = appId.program(programId.getType(), programId.getProgram());
      if (!getExistingAppProgramStatus(appSpec, pId).equals(ProgramStatus.STOPPED)) {
        return false;
      }
    }
    return true;
  }

  private boolean isConcurrentRunsInSameAppForbidden(ProgramType type) {
    // Concurrent runs in different (or same) versions of an application are forbidden for worker and flow
    return EnumSet.of(ProgramType.WORKER, ProgramType.FLOW).contains(type);
  }

  private boolean isConcurrentRunsAllowed(ProgramType type) {
    // Concurrent runs are only allowed for the Workflow, MapReduce and Spark
    return EnumSet.of(ProgramType.WORKFLOW, ProgramType.MAPREDUCE, ProgramType.SPARK).contains(type);
  }

  private Map<RunId, ProgramRuntimeService.RuntimeInfo> findRuntimeInfo(
    ProgramId programId, @Nullable String runId) throws BadRequestException {

    if (runId != null) {
      RunId run;
      try {
        run = RunIds.fromString(runId);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Error parsing run-id.", e);
      }
      ProgramRuntimeService.RuntimeInfo runtimeInfo = runtimeService.lookup(programId, run);
      return runtimeInfo == null ? Collections.emptyMap() : Collections.singletonMap(run, runtimeInfo);
    }
    return new HashMap<>(runtimeService.list(programId));
  }

  @Nullable
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId) throws BadRequestException {
    return findRuntimeInfo(programId, null).values().stream().findFirst().orElse(null);
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
   * Helper method to update log levels for Worker, Flow, or Service.
   */
  private void updateLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                               @Nullable String component, @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values().stream()
                                                                                     .findFirst().orElse(null);
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
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values().stream()
                                                                                     .findFirst().orElse(null);
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

  /**
   * Returns the active run records (PENDING / STARTING / RUNNING / SUSPENDED) based on the given program id and an
   * optional run id.
   */
  private Map<ProgramRunId, RunRecordMeta> getActiveRuns(ProgramId programId, @Nullable String runId) {
    if (runId == null) {
      return store.getActiveRuns(programId);
    }
    RunRecordMeta runRecord = store.getRun(programId.run(runId));
    EnumSet<ProgramRunStatus> activeStates = EnumSet.of(ProgramRunStatus.PENDING,
                                                        ProgramRunStatus.STARTING,
                                                        ProgramRunStatus.RUNNING,
                                                        ProgramRunStatus.SUSPENDED);
    return runRecord == null || !activeStates.contains(runRecord.getStatus())
      ? Collections.emptyMap()
      : Collections.singletonMap(programId.run(runId), runRecord);
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program} without performing
   * authorization enforcement.
   *
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}
   */
  @Nullable
  private ProgramSpecification getProgramSpecificationWithoutAuthz(ProgramId programId) {
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    if (appSpec == null) {
      return null;
    }
    return getExistingAppProgramSpecification(appSpec, programId);
  }

  private Set<PluginRequirement> getPluginRequirements(ProgramSpecification programSpecification) {
    return programSpecification.getPlugins().values()
      .stream().map(plugin -> new PluginRequirement(plugin.getPluginClass().getName(),
                                                    plugin.getPluginClass().getType(),
                                                    plugin.getPluginClass().getRequirements()))
      .collect(Collectors.toSet());
  }
  
  private boolean checkUserIdentityPropagationPreference(ProgramId programId) {
    Map<String, String> prefs = preferencesService.getResolvedProperties(programId);
    String key = SystemArguments.USER_IMPERSONATION_ENABLED;
    if (prefs.containsKey(key)) {
      if (prefs.get(key).equalsIgnoreCase("true")) {
        return true;
      }
    }
    return false;
  }
}
