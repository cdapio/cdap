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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractProgramLifecycleService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramLifecycleService.class);

  protected final Store store;
  protected final ProgramStateWriter programStateWriter;
  protected final AccessEnforcer accessEnforcer;
  protected final AuthenticationContext authenticationContext;
  protected final PropertiesResolver propertiesResolver;
  protected final CapabilityReader capabilityReader;
  protected final ArtifactRepository artifactRepository;

  @Inject
  protected AbstractProgramLifecycleService(Store store, ProgramStateWriter programStateWriter,
      AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext,
      PropertiesResolver propertiesResolver, CapabilityReader capabilityReader,
      ArtifactRepository artifactRepository) {
    this.store = store;
    this.programStateWriter = programStateWriter;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.propertiesResolver = propertiesResolver;
    this.capabilityReader = capabilityReader;
    this.artifactRepository = artifactRepository;
  }

  protected abstract void checkConcurrentExecution(ProgramId programId) throws Exception;

  /**
   * Adds {@link Constants#APP_CDAP_VERSION} system argument to the argument map if known.
   *
   * @param programId program that corresponds to application with version information
   * @param systemArgs map to add version information to
   */
  protected void addAppCdapVersion(ProgramId programId, Map<String, String> systemArgs) {
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    if (appSpec != null) {
      String appCDAPVersion = appSpec.getAppCDAPVersion();
      if (appCDAPVersion != null) {
        systemArgs.put(Constants.APP_CDAP_VERSION, appCDAPVersion);
      }
    }
  }

  protected void authorizePipelineRuntimeImpersonation(Map<String, String> userArgs) {
    if ((userArgs.containsKey(SystemArguments.RUNTIME_PRINCIPAL_NAME))
        && (userArgs.containsKey(SystemArguments.RUNTIME_KEYTAB_PATH))) {
      String principal = userArgs.get(SystemArguments.RUNTIME_PRINCIPAL_NAME);
      LOG.debug("Checking authorisation for user: {}, using runtime config principal: {}",
          authenticationContext.getPrincipal(), principal);
      KerberosPrincipalId kid = new KerberosPrincipalId(principal);
      accessEnforcer.enforce(kid, authenticationContext.getPrincipal(),
          AccessPermission.IMPERSONATE);
    }
  }

  protected void checkCapability(ProgramDescriptor programDescriptor) throws Exception {
    //check for capability at application class level
    Set<ApplicationClass> applicationClasses = artifactRepository
        .getArtifact(Id.Artifact.fromEntityId(programDescriptor.getArtifactId())).getMeta()
        .getClasses()
        .getApps();
    for (ApplicationClass applicationClass : applicationClasses) {
      Set<String> capabilities = applicationClass.getRequirements().getCapabilities();
      capabilityReader.checkAllEnabled(capabilities);
    }
    for (Map.Entry<String, Plugin> pluginEntry : programDescriptor.getApplicationSpecification()
        .getPlugins()
        .entrySet()) {
      Set<String> capabilities = pluginEntry.getValue().getPluginClass().getRequirements()
          .getCapabilities();
      capabilityReader.checkAllEnabled(capabilities);
    }
  }

  /**
   * Returns the program status.
   *
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
   *
   * @param appSpec the ApplicationSpecification of the existing application
   * @param programId the id of the program for which the status call is made
   * @return the status of the program
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  protected ProgramStatus getExistingAppProgramStatus(ApplicationSpecification appSpec,
      ProgramId programId) throws Exception {
    accessEnforcer.enforce(programId, authenticationContext.getPrincipal(), StandardPermission.GET);
    ProgramSpecification spec = getExistingAppProgramSpecification(appSpec,
        programId.getProgramReference());
    if (spec == null) {
      // program doesn't exist
      throw new NotFoundException(programId);
    }

    return getProgramStatus(store.getActiveRuns(programId).values());
  }

  /**
   * Returns the {@link ProgramSpecification} for the specified {@link ProgramId program}.
   *
   * @param appSpec the {@link ApplicationSpecification} of the existing application
   * @param programReference the {@link ProgramReference program} for which the {@link
   *     ProgramSpecification} is requested
   * @return the {@link ProgramSpecification} for the specified {@link ProgramId program}, or {@code
   *     null} if it does not exist
   */
  @Nullable
  protected ProgramSpecification getExistingAppProgramSpecification(ApplicationSpecification appSpec,
      ProgramReference programReference) {
    String programName = programReference.getProgram();
    ProgramType type = programReference.getType();
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
   * Returns the program status based on the active run records of a program. A program is RUNNING
   * if there are any RUNNING, STOPPING, or SUSPENDED run records. A program is starting if there
   * are any PENDING or STARTING run records and no RUNNING run records. Otherwise, it is STOPPED.
   *
   * @param runRecords run records for the program
   * @return the program status
   */
  @VisibleForTesting
  static ProgramStatus getProgramStatus(Collection<RunRecordDetail> runRecords) {
    boolean hasStarting = false;
    for (RunRecordDetail runRecord : runRecords) {
      ProgramRunStatus runStatus = runRecord.getStatus();
      if (runStatus == ProgramRunStatus.RUNNING || runStatus == ProgramRunStatus.SUSPENDED
          || runStatus == ProgramRunStatus.STOPPING) {
        return ProgramStatus.RUNNING;
      }
      hasStarting = hasStarting || runStatus == ProgramRunStatus.STARTING
          || runStatus == ProgramRunStatus.PENDING;
    }
    return hasStarting ? ProgramStatus.STARTING : ProgramStatus.STOPPED;
  }

  protected boolean isConcurrentRunsInSameAppForbidden(ProgramType type) {
    // Concurrent runs in different (or same) versions of an application are forbidden for worker
    return EnumSet.of(ProgramType.WORKER).contains(type);
  }

  protected boolean isConcurrentRunsAllowed(ProgramType type) {
    // Concurrent runs are only allowed for the Workflow, MapReduce and Spark
    return EnumSet.of(ProgramType.WORKFLOW, ProgramType.MAPREDUCE, ProgramType.SPARK)
        .contains(type);
  }

  /**
   * Returns whether the given program is stopped in all versions of the app.
   *
   * @param programId the id of the program for which the stopped status in all versions of the
   *     app is found
   * @return whether the given program is stopped in all versions of the app
   * @throws NotFoundException if the application to which this program belongs was not found
   */
  protected boolean isStoppedInSameProgram(ProgramId programId) throws Exception {
    // check that app exists
    Collection<ApplicationId> appIds = store.getAllAppVersionsAppIds(
        programId.getParent().getAppReference());
    if (appIds == null || appIds.isEmpty()) {
      throw new NotFoundException(
          Id.Application.from(programId.getNamespace(), programId.getApplication()));
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

  protected boolean isStopped(ProgramId programId) throws Exception {
    return ProgramStatus.STOPPED == getProgramStatus(programId);
  }
}
