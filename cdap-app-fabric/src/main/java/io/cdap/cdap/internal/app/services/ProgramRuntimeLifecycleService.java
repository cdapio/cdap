/*
 * Copyright © 2025 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
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
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.proto.ProgramRunClusterStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.twill.api.RunId;
import org.apache.twill.api.logging.LogEntry;

public class ProgramRuntimeLifecycleService extends AbstractProgramLifecycleService {

  private final ProgramRuntimeService runtimeService;

  @Inject
  public ProgramRuntimeLifecycleService(ProgramRuntimeService runtimeService, Store store,
      AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext,
      PropertiesResolver propertiesResolver, ProgramStateWriter programStateWriter,
      CapabilityReader capabilityReader, ArtifactRepository artifactRepository) {
    super(store, programStateWriter, accessEnforcer, authenticationContext, propertiesResolver,
        capabilityReader, artifactRepository);
    this.runtimeService = runtimeService;
  }

  @Override
  public void checkConcurrentExecution(ProgramId programId) throws Exception {
    if (isConcurrentRunsInSameAppForbidden(programId.getType())) {
      Map<RunId, RuntimeInfo> runs = runtimeService.list(programId);
      if (!runs.isEmpty() || !isStoppedInSameProgram(programId)) {
        throw new ConflictException(
            String.format(
                "Program %s is already running in an version of the same application with run ids %s",
                programId, runs.keySet()));
      }
    }
    if (!isConcurrentRunsAllowed(programId.getType())) {
      List<RunId> runIds = new ArrayList<>();
      for (Map.Entry<RunId, RuntimeInfo> entry : runtimeService.list(programId.getType())
          .entrySet()) {
        if (programId.equals(entry.getValue().getProgramId())) {
          runIds.add(entry.getKey());
        }
      }
      if (!runIds.isEmpty() || !isStopped(programId)) {
        throw new ConflictException(
            String.format("Program %s is already running with run ids %s", programId, runIds));
      }
    }
  }

  /**
   * Starts a Program run with the given arguments. This method skips cluster lifecycle steps and
   * does not perform authorization checks. If the program is already started, returns the
   * controller for the program. NOTE: This method should only be used from this service and the
   * {@link ProgramNotificationSubscriberService} upon receiving a {@link
   * ProgramRunClusterStatus#PROVISIONED} state.
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
   * Starts a Program with the specified argument overrides, skipping cluster lifecycle steps in the
   * run. NOTE: This method should only be called from preview runner.
   *
   * @param programId the {@link ProgramId} to start/stop
   * @param overrides the arguments to override in the program's configured user arguments
   *     before starting
   * @param debug {@code true} if the program is to be started in debug mode, {@code false}
   *     otherwise
   * @param isPreview true if the program is for preview run, for preview run, the app is
   *     already deployed with resolved properties, so no need to regenerate app spec again
   * @return {@link ProgramController}
   * @throws ConflictException if the specified program is already running, and if concurrent
   *     runs are not allowed
   * @throws NotFoundException if the specified program or the app it belongs to is not found in
   *     the specified namespace
   * @throws IOException if there is an error starting the program
   * @throws UnauthorizedException if the logged in user is not authorized to start the program.
   *     To start a program, a user requires {@link ApplicationPermission#EXECUTE} on the program
   * @throws Exception if there were other exceptions checking if the current user is authorized
   *     to start the program
   */
  public ProgramController start(ProgramId programId, Map<String, String> overrides, boolean debug,
      boolean isPreview) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(),
        ApplicationPermission.EXECUTE);
    checkConcurrentExecution(programId);

    Map<String, String> sysArgs = propertiesResolver.getSystemProperties(programId);
    addAppCdapVersion(programId, sysArgs);
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
    ProgramOptions options = new SimpleProgramOptions(programId, systemArguments, userArguments,
        debug);
    ProgramDescriptor programDescriptor = store.loadProgram(programId);
    ProgramRunId programRunId = programId.run(RunIds.generate());

    checkCapability(programDescriptor);

    programStateWriter.start(programRunId, options, null, programDescriptor);
    return startInternal(programDescriptor, options, programRunId);
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
      return runtimeInfo == null ? Collections.emptyMap()
          : Collections.singletonMap(run, runtimeInfo);
    }
    return new HashMap<>(runtimeService.list(programId));
  }

  @Nullable
  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(ProgramId programId)
      throws BadRequestException {
    return findRuntimeInfo(programId, null).values().stream().findFirst().orElse(null);
  }

  /**
   * Set instances for the given program. Only supported program types for this action are {@link
   * ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which instances are to be
   *     updated
   * @param instances the number of instances to be updated.
   * @throws InterruptedException if there is an error while asynchronously updating instances
   * @throws ExecutionException if there is an error while asynchronously updating instances
   * @throws BadRequestException if the number of instances specified is less than 0
   * @throws UnauthorizedException if the user does not have privileges to set instances for the
   *     specified program. To set instances for a program, a user needs {@link
   *     StandardPermission#UPDATE} on the program.
   */
  public void setInstances(ProgramId programId, int instances) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(),
        StandardPermission.UPDATE);
    if (instances < 1) {
      throw new BadRequestException(
          String.format("Instance count should be greater than 0. Got %s.", instances));
    }
    switch (programId.getType()) {
      case SERVICE:
        setServiceInstances(programId, instances);
        break;
      case WORKER:
        setWorkerInstances(programId, instances);
        break;
      default:
        throw new BadRequestException(
            String.format("Setting instances for program type %s is not supported",
                programId.getType().getPrettyName()));
    }
  }

  /**
   * Reset log levels for the given program. Only supported program types for this action are {@link
   * ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be
   *     reset.
   * @param loggerNames the {@link String} set of the logger names to be updated, empty means
   *     reset for all loggers.
   * @param runId the run id of the program.
   * @throws InterruptedException if there is an error while asynchronously resetting log
   *     levels.
   * @throws ExecutionException if there is an error while asynchronously resetting log levels.
   * @throws UnauthorizedException if the user does not have privileges to reset log levels for
   *     the specified program. To reset log levels for a program, a user needs {@link
   *     StandardPermission#UPDATE} on the program.
   */
  public void resetProgramLogLevels(ProgramId programId, Set<String> loggerNames,
      @Nullable String runId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(),
        StandardPermission.UPDATE);
    if (!EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(
          String.format("Resetting log levels for program type %s is not supported",
              programId.getType().getPrettyName()));
    }
    resetLogLevels(programId, loggerNames, runId);
  }

  /**
   * Update log levels for the given program. Only supported program types for this action are
   * {@link ProgramType#SERVICE} and {@link ProgramType#WORKER}.
   *
   * @param programId the {@link ProgramId} of the program for which log levels are to be
   *     updated
   * @param logLevels the {@link Map} of the log levels to be updated.
   * @param runId the run id of the program.
   * @throws InterruptedException if there is an error while asynchronously updating log
   *     levels.
   * @throws ExecutionException if there is an error while asynchronously updating log levels.
   * @throws BadRequestException if the log level is not valid or the program type is not
   *     supported.
   * @throws UnauthorizedException if the user does not have privileges to update log levels for
   *     the specified program. To update log levels for a program, a user needs {@link
   *     StandardPermission#UPDATE} on the program.
   */
  public void updateProgramLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
      @Nullable String runId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(),
        StandardPermission.UPDATE);
    if (!EnumSet.of(ProgramType.SERVICE, ProgramType.WORKER).contains(programId.getType())) {
      throw new BadRequestException(
          String.format("Updating log levels for program type %s is not supported",
              programId.getType().getPrettyName()));
    }
    updateLogLevels(programId, logLevels, runId);
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

  /**
   * Helper method to get the {@link LogLevelUpdater} for the program.
   */
  private LogLevelUpdater getLogLevelUpdater(RuntimeInfo runtimeInfo) throws Exception {
    ProgramController programController = runtimeInfo.getController();
    if (!(programController instanceof LogLevelUpdater)) {
      throw new BadRequestException(
          "Update log levels at runtime is only supported in distributed mode");
    }
    return ((LogLevelUpdater) programController);
  }

  /**
   * Helper method to update log levels for Worker or Service.
   */
  private void updateLogLevels(ProgramId programId, Map<String, LogEntry.Level> logLevels,
      @Nullable String runId) throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values()
        .stream()
        .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.updateLogLevels(logLevels, null);
    }
  }

  /**
   * Helper method to reset log levels for Worker or Service.
   */
  private void resetLogLevels(ProgramId programId, Set<String> loggerNames, @Nullable String runId)
      throws Exception {
    ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId, runId).values()
        .stream()
        .findFirst().orElse(null);
    if (runtimeInfo != null) {
      LogLevelUpdater logLevelUpdater = getLogLevelUpdater(runtimeInfo);
      logLevelUpdater.resetLogLevels(loggerNames, null);
    }
  }
}
