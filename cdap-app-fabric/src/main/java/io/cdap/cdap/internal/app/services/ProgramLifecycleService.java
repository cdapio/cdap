/*
 * Copyright © 2015-2020 Cask Data, Inc.
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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.LogLevelUpdater;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.pipeline.PluginRequirement;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.provision.ProvisioningOp;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.internal.provision.ProvisioningTaskInfo;
import io.cdap.cdap.proto.ProgramHistory;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.RunCountResult;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerDetail;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.runtime.spi.profile.ProfileStatus;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

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
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final CapabilityReader capabilityReader;
  private final int maxConcurrentRuns;
  private final ArtifactRepository artifactRepository;

  @Inject
  ProgramLifecycleService(CConfiguration cConf,
                          Store store, ProfileService profileService, ProgramRuntimeService runtimeService,
                          PropertiesResolver propertiesResolver,
                          PreferencesService preferencesService, AccessEnforcer accessEnforcer,
                          AuthenticationContext authenticationContext,
                          ProvisionerNotifier provisionerNotifier, ProvisioningService provisioningService,
                          ProgramStateWriter programStateWriter, CapabilityReader capabilityReader,
                          ArtifactRepository artifactRepository) {
    this.maxConcurrentRuns = cConf.getInt(Constants.AppFabric.MAX_CONCURRENT_RUNS);
    this.store = store;
    this.profileService = profileService;
    this.runtimeService = runtimeService;
    this.propertiesResolver = propertiesResolver;
    this.preferencesService = preferencesService;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.provisionerNotifier = provisionerNotifier;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.capabilityReader = capabilityReader;
    this.artifactRepository = artifactRepository;
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
   * Gets the {@link ProgramStatus} for the given set of programs.
   *
   * @param programIds collection of program ids for retrieving status
   * @return a {@link Map} from the {@link ProgramId} to the corresponding status; there will be no entry for programs
   * that do not exist.
   */
  public Map<ProgramId, ProgramStatus> getProgramStatuses(Collection<ProgramId> programIds) throws Exception {
    // filter the result
    Set<? extends EntityId> visibleEntities = accessEnforcer.isVisible(new LinkedHashSet<>(programIds),
                                                                              authenticationContext.getPrincipal());
    List<ProgramId> filteredIds = programIds.stream().filter(visibleEntities::contains).collect(Collectors.toList());

    Map<ProgramId, ProgramStatus> result = new HashMap<>();
    for (Map.Entry<ProgramId, Collection<RunRecordDetail>> entry : store.getActiveRuns(filteredIds).entrySet()) {
      result.put(entry.getKey(), getProgramStatus(entry.getValue()));
    }
    return result;
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
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
    return store.getProgramRunCount(programId);
  }

  /**
   * Returns the program run count of the given program id list.
   *
   * @param programIds the list of program ids to get the count
   * @return the counts of given program ids
   */
  public List<RunCountResult> getProgramRunCounts(List<ProgramId> programIds) throws Exception {
    // filter the result
    Principal principal = authenticationContext.getPrincipal();
    Set<? extends EntityId> visibleEntities = accessEnforcer.isVisible(new HashSet<>(programIds), principal);
    Set<ProgramId> filteredIds = programIds.stream().filter(visibleEntities::contains).collect(Collectors.toSet());

    Map<ProgramId, RunCountResult> programCounts = store.getProgramRunCounts(filteredIds).stream()
      .collect(Collectors.toMap(RunCountResult::getProgramId, c -> c));

    List<RunCountResult> result = new ArrayList<>();
    for (ProgramId programId : programIds) {
      if (!visibleEntities.contains(programId)) {
        result.add(new RunCountResult(programId, null, new UnauthorizedException(principal, programId)));
      } else {
        RunCountResult count = programCounts.get(programId);
        if (count != null) {
          result.add(count);
        } else {
          result.add(new RunCountResult(programId, 0L, null));
        }
      }
    }
    return result;
  }

  /**
   * Returns the {@link RunRecordDetail} for the given program run.
   *
   * @param programRunId the program run to fetch
   * @return the {@link RunRecordDetail} for the given run
   * @throws NotFoundException if the given program or program run doesn't exist
   * @throws Exception if authorization failed
   */
  public RunRecordDetail getRunRecordMeta(ProgramRunId programRunId) throws Exception {
    accessEnforcer.enforce(programRunId, authenticationContext.getPrincipal(), StandardPermission.GET);

    ProgramSpecification programSpec = getProgramSpecificationWithoutAuthz(programRunId.getParent());
    if (programSpec == null) {
      throw new NotFoundException(programRunId.getParent());
    }
    RunRecordDetail meta = store.getRun(programRunId);
    if (meta == null) {
      throw new NotFoundException(programRunId);
    }
    return meta;
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
  public List<RunRecordDetail> getRunRecordMetas(ProgramId programId, ProgramRunStatus programRunStatus,
                                                 long start, long end, int limit) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
    ProgramSpecification programSpec = getProgramSpecificationWithoutAuthz(programId);
    if (programSpec == null) {
      throw new NotFoundException(programId);
    }
    return store.getRuns(programId, programRunStatus, start, end, limit).values().stream().collect(Collectors.toList());
  }

  public List<RunRecord> getRunRecords(ProgramId programId, ProgramRunStatus programRunStatus,
                                       long start, long end, int limit) throws Exception {
    return getRunRecordMetas(programId, programRunStatus, start, end, limit).stream()
      .map(record -> RunRecord.builder(record).build()).collect(Collectors.toList());
  }

  /**
   * Get the latest runs within the specified start and end times for the specified programs.
   *
   * @param programs the programs to get runs for
   * @param programRunStatus status of runs to return
   * @param start earliest start time of runs to return
   * @param end latest start time of runs to return
   * @param limit the maximum number of runs per program to return
   * @return the latest runs for the program sorted by start time, with the newest run as the first run
   * @throws NotFoundException if the application to which this program belongs was not found or the program is not
   *                           found in the app
   * @throws UnauthorizedException if the principal does not have access to the program
   * @throws Exception if there was some other exception performing authorization checks
   */
  public List<ProgramHistory> getRunRecords(Collection<ProgramId> programs, ProgramRunStatus programRunStatus,
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
    Set<? extends EntityId> visibleEntities = accessEnforcer.isVisible(new HashSet<>(programs),
                                                                              authenticationContext.getPrincipal());
    for (ProgramHistory programHistory : store.getRuns(programs, programRunStatus, start, end, limit)) {
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
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
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
  static ProgramStatus getProgramStatus(Collection<RunRecordDetail> runRecords) {
    boolean hasStarting = false;
    for (RunRecordDetail runRecord : runRecords) {
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
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}, or {@code null} if it does
   *         not exist
   */
  @Nullable
  public ProgramSpecification getProgramSpecification(ProgramId programId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
    return getProgramSpecificationWithoutAuthz(programId);
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programId the {@link ProgramId program} for which the {@link ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}, or {@code null} if it does
   *         not exist
   */
  @Nullable
  private ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec,
                                                                  ProgramId programId) {
    String programName = programId.getProgram();
    ProgramType type = programId.getType();
    ProgramSpecification programSpec;
    if (type == ProgramType.MAPREDUCE && appSpec.getMapReduce().containsKey(programName)) {
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
   *                               a user requires {@link ApplicationPermission#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public RunId run(ProgramId programId, Map<String, String> overrides, boolean debug) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);
    checkConcurrentExecution(programId);

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(programId);
    Map<String, String> userArgs = propertiesResolver.getUserProperties(programId);
    if (overrides != null) {
      userArgs.putAll(overrides);
    }

    authorizePipelineRuntimeImpersonation(userArgs);

    return runInternal(programId, userArgs, sysArgs, debug);
  }

  /**
   * Starts programs which were suspended between startTime and endTime.
   *
   * @param applicationId the application to restart programs in
   * @param startTimeSeconds earliest stop time in seconds of programs to restart
   * @param endTimeSeconds latest stop time in seconds of programs to restart
   * @return {@link Set<RunId>} of runs restarted
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link ApplicationPermission#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public Set<RunId> restart(ApplicationId applicationId, long startTimeSeconds, long endTimeSeconds) throws Exception {
    Set<RunId> runs = new HashSet<>();
    Map<ProgramRunId, RunRecordDetail> runMap =
      store.getRuns(applicationId, ProgramRunStatus.KILLED, Integer.MAX_VALUE, meta -> {
        Long stopTime = meta.getStopTs();
        return stopTime != null && stopTime >= startTimeSeconds && stopTime < endTimeSeconds;
      });

    for (ProgramRunId programRunId : runMap.keySet()) {
      ProgramId programId = programRunId.getParent();
      runs.add(run(programId, getRuntimeArgs(programId), false));
    }

    return runs;
  }

  /**
   * Stop all active programs for the given application
   * @param applicationId
   * @throws Exception
   */
  public void stopAll(ApplicationId applicationId) throws Exception {
    Map<ProgramRunId, RunRecordDetail> runMap = store.getActiveRuns(applicationId);
    for (ProgramRunId programRunId : runMap.keySet()) {
      stop(programRunId.getParent(), programRunId.getRun());
    }
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
   * @throws Exception if there were other exceptions
   */
  public RunId runInternal(ProgramId programId, Map<String, String> userArgs, Map<String, String> sysArgs,
                           boolean debug) throws Exception {
    RunId runId = RunIds.generate();
    ProgramOptions programOptions = createProgramOptions(programId, userArgs, sysArgs, debug);
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    String userId = SecurityRequestContext.getUserId();
    userId = userId == null ? "" : userId;

    checkCapability(programDescriptor);
    synchronized (this) {
      if (maxConcurrentRuns > 0 && maxConcurrentRuns <= store.countActiveRuns(maxConcurrentRuns)) {
        ConflictException e = new ConflictException(
          String.format("Program %s cannot start because the maximum of %d concurrent runs is exceeded",
                        programId, maxConcurrentRuns));

        programStateWriter.reject(programId.run(runId), programOptions, programDescriptor, userId, e);
        throw e;
      }
    }

    LOG.info("Attempt to run {} program {} as user {} with arguments {}", programId.getType(), programId.getProgram(),
             authenticationContext.getPrincipal().getName(), userArgs);

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
    addAppCDAPVersion(programId, systemArgs);
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
   * @param isPreview true if the program is for preview run, for preview run, the app is already deployed with resolved
   *                  properties, so no need to regenerate app spec again
   * @return {@link ProgramController}
   * @throws ConflictException if the specified program is already running, and if concurrent runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program. To start a program,
   *                               a user requires {@link ApplicationPermission#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized to start the program
   */
  public ProgramController start(ProgramId programId, Map<String, String> overrides, boolean debug,
                                 boolean isPreview) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);
    checkConcurrentExecution(programId);

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(programId);
    addAppCDAPVersion(programId, sysArgs);
    sysArgs.put(ProgramOptionConstants.SKIP_PROVISIONING, "true");
    sysArgs.put(SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
    sysArgs.put(ProgramOptionConstants.IS_PREVIEW, Boolean.toString(isPreview));
    Map<String, String> userArgs = propertiesResolver.getUserProperties(programId);
    if (overrides != null) {
      userArgs.putAll(overrides);
    }

    authorizePipelineRuntimeImpersonation(userArgs);

    BasicArguments systemArguments = new BasicArguments(sysArgs);
    BasicArguments userArguments = new BasicArguments(userArgs);
    ProgramOptions options = new SimpleProgramOptions(programId, systemArguments, userArguments, debug);
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    ProgramRunId programRunId = programId.run(RunIds.generate());

    checkCapability(programDescriptor);

    programStateWriter.start(programRunId, options, null, programDescriptor);
    return startInternal(programDescriptor, options, programRunId);
  }

  private void checkCapability(ProgramDescriptor programDescriptor) throws Exception {
    //check for capability at application class level
    Set<ApplicationClass> applicationClasses = artifactRepository
      .getArtifact(Id.Artifact.fromEntityId(programDescriptor.getArtifactId())).getMeta().getClasses()
      .getApps();
    for (ApplicationClass applicationClass : applicationClasses) {
      Set<String> capabilities = applicationClass.getRequirements().getCapabilities();
      capabilityReader.checkAllEnabled(capabilities);
    }
    for (Map.Entry<String, Plugin> pluginEntry : programDescriptor.getApplicationSpecification().getPlugins()
      .entrySet()) {
      Set<String> capabilities = pluginEntry.getValue().getPluginClass().getRequirements().getCapabilities();
      capabilityReader.checkAllEnabled(capabilities);
    }
  }

  /**
   * Starts a Program run with the given arguments. This method skips cluster lifecycle steps and
   * does not perform authorization checks. If the program is already started, returns the controller for the program.
   * NOTE: This method should only be used from this service and the {@link ProgramNotificationSubscriberService}
   * upon receiving a {@link ProgramRunClusterStatus#PROVISIONED} state.
   *
   * @param programDescriptor descriptor of the program to run
   * @param programOptions options for the program run
   * @param programRunId program run id
   * @return controller for the program
   */
  ProgramController startInternal(ProgramDescriptor programDescriptor,
                                  ProgramOptions programOptions, ProgramRunId programRunId) {
    RunId runId = RunIds.fromString(programRunId.getRun());

    synchronized (this) {
      RuntimeInfo runtimeInfo = runtimeService.lookup(programRunId.getParent(), runId);
      if (runtimeInfo != null) {
        return runtimeInfo.getController();
      }
      return runtimeService.run(programDescriptor, programOptions, runId).getController();
    }
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
  public void stop(ProgramId programId) throws Exception {
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
   *                               program, a user requires {@link ApplicationPermission#EXECUTE} permission on
   *                               the program.
   */
  public List<ListenableFuture<ProgramRunId>> issueStop(ProgramId programId, @Nullable String runId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), ApplicationPermission.EXECUTE);

    // See if the program is running as per the runtime service
    Map<RunId, RuntimeInfo> runtimeInfos = findRuntimeInfo(programId, runId);
    Map<ProgramRunId, RunRecordDetail> activeRunRecords = getActiveRuns(programId, runId);

    if (runtimeInfos.isEmpty() && activeRunRecords.isEmpty()) {
      // Error out if no run information from runtime service and from run record
      Store.ensureProgramExists(programId, store.getApplication(programId.getParent()));
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
        RunRecordDetail runRecord = activeRunRecords.get(activeRunId);
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
   *                               {@link StandardPermission#UPDATE} privileges on the program.
   */
  public void saveRuntimeArgs(ProgramId programId, Map<String, String> runtimeArgs) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    Store.ensureProgramExists(programId, store.getApplication(programId.getParent()));
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
   * {@link StandardPermission#GET} privileges on the program.
   */
  public Map<String, String> getRuntimeArgs(@Name("programId") ProgramId programId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
    Store.ensureProgramExists(programId, store.getApplication(programId.getParent()));
    return preferencesService.getProperties(programId);
  }

  /**
   * Update log levels for the given program. Only supported program types for this action are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be updated
   * @param logLevels the {@link Map} of the log levels to be updated.
   * @param runId the run id of the program.
   * @throws InterruptedException if there is an error while asynchronously updating log levels.
   * @throws ExecutionException if there is an error while asynchronously updating log levels.
   * @throws BadRequestException if the log level is not valid or the program type is not supported.
   * @throws UnauthorizedException if the user does not have privileges to update log levels for the specified program.
   *                               To update log levels for a program, a user needs {@link StandardPermission#UPDATE}
   *                               on the program.
   */
  public void updateProgramLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                                     @Nullable String runId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (!EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(String.format("Updating log levels for program type %s is not supported",
                                                  programId.getType().getPrettyName()));
    }
    updateLogLevels(programId, logLevels, runId);
  }

  /**
   * Reset log levels for the given program. Only supported program types for this action are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be reset.
   * @param loggerNames the {@link String} set of the logger names to be updated, empty means reset for all
   *                    loggers.
   * @param runId the run id of the program.
   * @throws InterruptedException if there is an error while asynchronously resetting log levels.
   * @throws ExecutionException if there is an error while asynchronously resetting log levels.
   * @throws UnauthorizedException if the user does not have privileges to reset log levels for the specified program.
   *                               To reset log levels for a program, a user needs {@link StandardPermission#UPDATE}
   *                               on the program.
   */
  public void resetProgramLogLevels(ProgramId programId, Set<String> loggerNames,
                                    @Nullable String runId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    if (!EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(String.format("Resetting log levels for program type %s is not supported",
                                                  programId.getType().getPrettyName()));
    }
    resetLogLevels(programId, loggerNames, runId);
  }

  /**
   * Ensures the caller is authorized to check if the given program exists.
   */
  public void ensureProgramExists(ProgramId programId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
    Store.ensureProgramExists(programId, store.getApplication(programId.getParent()));
  }

  private boolean isStopped(ProgramId programId) throws Exception {
    return ProgramStatus.STOPPED == getProgramStatus(programId);
  }

  /**
   * Checks if the given program is running and is allowed for concurrent execution.
   *
   * @param programId the program Id to check
   * @throws ConflictException if concurrent execution is not allowed and has an existing run
   * @throws NotFoundException if the program is not found
   * @throws Exception if failed to determine the state
   */
  private synchronized void checkConcurrentExecution(ProgramId programId) throws Exception {
    if (isConcurrentRunsInSameAppForbidden(programId.getType())) {
      Map<RunId, RuntimeInfo> runs = runtimeService.list(programId);
      if (!runs.isEmpty() || !isStoppedInSameProgram(programId)) {
        throw new ConflictException(
          String.format("Program %s is already running in an version of the same application with run ids %s",
                        programId, runs.keySet()));
      }
    }
    if (!isConcurrentRunsAllowed(programId.getType())) {
      List<RunId> runIds = new ArrayList<>();
      for (Map.Entry<RunId, RuntimeInfo> entry : runtimeService.list(programId.getType()).entrySet()) {
        if (programId.equals(entry.getValue().getProgramId())) {
          runIds.add(entry.getKey());
        }
      }
      if (!runIds.isEmpty() || !isStopped(programId)) {
        throw new ConflictException(String.format("Program %s is already running with run ids %s", programId, runIds));
      }
    }
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
    // Concurrent runs in different (or same) versions of an application are forbidden for worker
    return EnumSet.of(ProgramType.WORKER).contains(type);
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
   * Set instances for the given program. Only supported program types for this action are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which instances are to be updated
   * @param instances the number of instances to be updated.
   * @throws InterruptedException if there is an error while asynchronously updating instances
   * @throws ExecutionException if there is an error while asynchronously updating instances
   * @throws BadRequestException if the number of instances specified is less than 0
   * @throws UnauthorizedException if the user does not have privileges to set instances for the specified program.
   *                               To set instances for a program, a user needs {@link StandardPermission#UPDATE}
   *                               on the program.
   */
  public void setInstances(ProgramId programId, int instances) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);
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
    return !accessEnforcer.isVisible(Collections.singleton(programId), principal).isEmpty();
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
   * Helper method to update log levels for Worker or Service.
   */
  private void updateLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
                               @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values().stream()
                                                                                     .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.updateLogLevels(logLevels, null);
    }
  }

  /**
   * Helper method to reset log levels for Worker or Service.
   */
  private void resetLogLevels(ProgramId programId, Set<String> loggerNames, @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values().stream()
                                                                                     .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.resetLogLevels(loggerNames, null);
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
  private Map<ProgramRunId, RunRecordDetail> getActiveRuns(ProgramId programId, @Nullable String runId) {
    if (runId == null) {
      return store.getActiveRuns(programId);
    }
    RunRecordDetail runRecord = store.getRun(programId.run(runId));
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

  /**
   * Adds {@link Constants#APP_CDAP_VERSION} system argument to the argument map if known.
   * @param programId program that corresponds to application with version information
   * @param systemArgs map to add version information to
   */
  public void addAppCDAPVersion(ProgramId programId, Map<String, String> systemArgs) {
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    if (appSpec != null) {
      String appCDAPVersion = appSpec.getAppCDAPVersion();
      if (appCDAPVersion != null) {
        systemArgs.put(Constants.APP_CDAP_VERSION, appCDAPVersion);
      }
    }
  }

  private Set<PluginRequirement> getPluginRequirements(ProgramSpecification programSpecification) {
    return programSpecification.getPlugins().values()
      .stream().map(plugin -> new PluginRequirement(plugin.getPluginClass().getName(),
                                                    plugin.getPluginClass().getType(),
                                                    plugin.getPluginClass().getRequirements()))
      .collect(Collectors.toSet());
  }

  private void authorizePipelineRuntimeImpersonation(Map<String, String> userArgs) throws Exception {
    if ((userArgs.containsKey(SystemArguments.RUNTIME_PRINCIPAL_NAME)) &&
            (userArgs.containsKey(SystemArguments.RUNTIME_KEYTAB_PATH))) {
      String principal = userArgs.get(SystemArguments.RUNTIME_PRINCIPAL_NAME);
      LOG.debug("Checking authorisation for user: {}, using runtime config principal: {}",
                authenticationContext.getPrincipal(), principal);
      KerberosPrincipalId kid = new KerberosPrincipalId(principal);
      accessEnforcer.enforce(kid, authenticationContext.getPrincipal(), AccessPermission.IMPERSONATE);
    }
  }
}
