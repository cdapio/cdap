/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.MethodNotAllowedException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.security.NamespacePermission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.NamespaceConfigData;
import io.cdap.cdap.proto.sourcecontrol.NamespaceConnectionConfig;
import io.cdap.cdap.proto.sourcecontrol.RemoteRepositoryValidationException;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.NoChangesToPullException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.SourceControlConfig;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.NamespaceRepository;
import io.cdap.cdap.sourcecontrol.operationrunner.PulAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushNamespaceConfigOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushNamespaceConfigResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.NamespaceTable;
import io.cdap.cdap.store.RepositoryTable;
import java.io.IOException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that manages source control for repositories and applications. It exposes repository CRUD
 * apis and source control tasks that do pull/pull/list applications in linked repository.
 */
public class SourceControlManagementService {

  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final TransactionRunner transactionRunner;
  private final CConfiguration cConf;
  private final SecureStore secureStore;
  private final SourceControlOperationRunner sourceControlOperationRunner;
  private final ApplicationLifecycleService appLifecycleService;
  private final PreferencesService preferencesService;
  private final ProgramScheduleService programScheduleService;

  private final Store store;
  private static final Logger LOG = LoggerFactory.getLogger(SourceControlManagementService.class);
  private static final List<Constraint> NO_CONSTRAINTS = Collections.emptyList();
  private final ProfileService profileService;
  private static final Gson GSON = new GsonBuilder().create();


  /**
   * Constructor for SourceControlManagementService with all params injected via guice.
   */
  @Inject
  public SourceControlManagementService(CConfiguration cConf,
                                        SecureStore secureStore,
                                        TransactionRunner transactionRunner,
                                        AccessEnforcer accessEnforcer,
                                        AuthenticationContext authenticationContext,
                                        SourceControlOperationRunner sourceControlOperationRunner,
                                        ApplicationLifecycleService applicationLifecycleService,
                                        ProfileService profileService,
                                        PreferencesService preferencesService,
                                        ProgramScheduleService programScheduleService,
                                        Store store) {
    this.cConf = cConf;
    this.secureStore = secureStore;
    this.transactionRunner = transactionRunner;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.sourceControlOperationRunner = sourceControlOperationRunner;
    this.appLifecycleService = applicationLifecycleService;
    this.programScheduleService = programScheduleService;
    this.profileService = profileService;
    this.preferencesService = preferencesService;
    this.store = store;
  }

  private RepositoryTable getRepositoryTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new RepositoryTable(context);
  }

  private NamespaceTable getNamespaceTable(StructuredTableContext context)
      throws TableNotFoundException {
    return new NamespaceTable(context);
  }

  /**
   * Add a repository config to a namespace.
   *
   * @param namespace {@link NamespaceId} to link repository to
   * @param repository {@link RepositoryConfig} for the linked repository
   * @return {@link RepositoryMeta} representing the linked repository
   * @throws NamespaceNotFoundException if the namespace is non-existent
   */
  public RepositoryMeta setRepository(NamespaceId namespace, RepositoryConfig repository)
      throws NamespaceNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.UPDATE_REPOSITORY_METADATA);

    return TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable nsTable = getNamespaceTable(context);
      if (nsTable.get(namespace) == null) {
        throw new NamespaceNotFoundException(namespace);
      }

      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.create(namespace, repository);
      return repoTable.get(namespace);
    }, NamespaceNotFoundException.class);
  }

  /**
   * Delete the current repository config from namespace.
   *
   * @param namespace {@link NamespaceId} do unlink repository from
   */
  public void deleteRepository(NamespaceId namespace) {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.UPDATE_REPOSITORY_METADATA);

    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable repoTable = getRepositoryTable(context);
      repoTable.delete(namespace);
    });
  }

  /**
   * Return the repository config for the given namespace.
   *
   * @throws RepositoryNotFoundException if repository config is not available for the
   *     namespace
   */
  public RepositoryMeta getRepositoryMeta(NamespaceId namespace)
      throws RepositoryNotFoundException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.GET);

    return TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable table = getRepositoryTable(context);
      RepositoryMeta repoMeta = table.get(namespace);
      if (repoMeta == null) {
        throw new RepositoryNotFoundException(namespace);
      }

      return repoMeta;
    }, RepositoryNotFoundException.class);
  }

  /**
   * Validates the remote repository config for the given namespace.
   *
   * @throws RemoteRepositoryValidationException if validation of remote repository fails
   */
  public void validateRepository(NamespaceId namespace, RepositoryConfig repoConfig)
      throws RemoteRepositoryValidationException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.UPDATE_REPOSITORY_METADATA);
    RepositoryManager.validateConfig(secureStore,
        new SourceControlConfig(namespace, repoConfig, cConf));
  }

  /**
   * The method to push the namespace config to linked repository.
   *
   * @param namespace {@link NamespaceId}
   * @param commitMessage enforced commit message from user
   * @return {@link PushNamespaceConfigResponse}
   * @throws NotFoundException if the namespace is not found or the repository config is not found
   * @throws IOException if {@link ApplicationLifecycleService} fails to get the adminOwner store
   * @throws SourceControlException if {@link SourceControlOperationRunner} fails to push
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   * @throws NoChangesToPushException if there's no change of the application between namespace and linked repository
   */
  public PushNamespaceConfigResponse pushNamespaceConfig(NamespaceId namespace, String commitMessage, NamespaceConnectionConfig[] connections)
      throws NotFoundException, IOException, NoChangesToPushException, AuthenticationConfigException {

    // TODO Add the accessEnforcement with correct permissions and uncomment the following line
    // accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.GET);

    // TODO: CDAP-20396 RepositoryConfig is currently only accessible from the service layer
    //  Need to fix it and avoid passing it in RepositoryManagerFactory
    RepositoryConfig repoConfig = getRepositoryMeta(namespace).getConfig();

    List<Profile> profiles = profileService.getProfiles(namespace, true);
    PreferencesDetail preferences = preferencesService.getPreferences(namespace);
    // TODO Figure out how to get the list of connections in a namespace.
    NamespaceConfigData nsConfig = new NamespaceConfigData(
        namespace.getEntityName(), profiles, preferences, connections);

    String committer = authenticationContext.getPrincipal().getName();
    // TODO CDAP-20371 revisit and put correct Author and Committer, for now they are the same
    CommitMeta commitMeta = new CommitMeta(
        committer, committer, System.currentTimeMillis(), commitMessage);

    LOG.info("Start to push namespace {} config to linked repository by user {}",
        namespace,
        appLifecycleService.decodeUserId(authenticationContext));

    PushNamespaceConfigResponse pushResponse = sourceControlOperationRunner.pushNamespaceConfig(
        new PushNamespaceConfigOperationRequest(namespace, repoConfig, nsConfig, commitMeta)
    );

    LOG.info("Successfully pushed namespace {} config to linked repository by user {}",
        namespace,
        appLifecycleService.decodeUserId(authenticationContext));

    SourceControlMeta sourceControlMeta = new SourceControlMeta(pushResponse.getFileHash());
    //ApplicationId appId = appRef.app(appDetail.getAppVersion());
    //store.setAppSourceControlMeta(appId, sourceControlMeta);

    return pushResponse;
  }

  /**
   * Pull the namespace config from linked repository and deploy it.
   *
   * @param appRef application reference to deploy with
   * @return {@link ApplicationRecord} of the deployed application.
   * @throws Exception when {@link ApplicationLifecycleService} fails to deploy.
   * @throws NoChangesToPullException if the fileHashes are the same
   * @throws NotFoundException if the repository config is not found or the application in repository is not found
   * @throws SourceControlException if unexpected errors happen when pulling the application.
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   */
  public NamespaceConfigData pullAndDeployNamespace(NamespaceId namespace) throws Exception {
    // Enforcing namespace CREATE permission and Namespace READ_REPOSITORY permission upfront
    // accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(), StandardPermission.CREATE);
    // accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
    //    NamespacePermission.READ_REPOSITORY);

    return null;
  }

  /**
   * The method to push an application to linked repository.
   *
   * @param appRef {@link ApplicationReference}
   * @param commitMessage enforced commit message from user
   * @return {@link PushAppResponse}
   * @throws NotFoundException if the application is not found or the repository config is not
   *     found
   * @throws IOException if {@link ApplicationLifecycleService} fails to get the adminOwner
   *     store
   * @throws SourceControlException if {@link SourceControlOperationRunner} fails to push
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   * @throws NoChangesToPushException if there's no change of the application between namespace
   *     and linked repository
   */
  public PushAppResponse pushApp(ApplicationReference appRef, String commitMessage)
      throws NotFoundException, IOException, NoChangesToPushException, AuthenticationConfigException {
    accessEnforcer.enforce(appRef.getParent(), authenticationContext.getPrincipal(),
        NamespacePermission.WRITE_REPOSITORY);

    // TODO: CDAP-20396 RepositoryConfig is currently only accessible from the service layer
    //  Need to fix it and avoid passing it in RepositoryManagerFactory
    RepositoryConfig repoConfig = getRepositoryMeta(appRef.getParent()).getConfig();

    // AppLifecycleService already enforces ApplicationDetail Access
    ApplicationDetail appDetail = appLifecycleService.getLatestAppDetail(appRef, false);

    String committer = authenticationContext.getPrincipal().getName();
    // TODO CDAP-20371 revisit and put correct Author and Committer, for now they are the same
    CommitMeta commitMeta = new CommitMeta(committer, committer, System.currentTimeMillis(),
        commitMessage);

    LOG.info("Start to push app {} in namespace {} to linked repository by user {}",
        appRef.getApplication(),
        appRef.getParent(),
        appLifecycleService.decodeUserId(authenticationContext));

    PushAppResponse pushResponse = sourceControlOperationRunner.push(
        new PushAppOperationRequest(appRef.getParent(), repoConfig, appDetail, commitMeta)
    );

    LOG.info("Successfully pushed app {} in namespace {} to linked repository by user {}",
        appRef.getApplication(),
        appRef.getParent(),
        appLifecycleService.decodeUserId(authenticationContext));

    SourceControlMeta sourceControlMeta = new SourceControlMeta(pushResponse.getFileHash());
    ApplicationId appId = appRef.app(appDetail.getAppVersion());
    store.setAppSourceControlMeta(appId, sourceControlMeta);

    return pushResponse;
  }

  /**
   * Pull the application from linked repository and deploy it in current namespace.
   *
   * @param appRef application reference to deploy with
   * @return {@link ApplicationRecord} of the deployed application.
   * @throws Exception when {@link ApplicationLifecycleService} fails to deploy.
   * @throws NoChangesToPullException if the fileHashes are the same
   * @throws NotFoundException if the repository config is not found or the application in
   *     repository is not found
   * @throws SourceControlException if unexpected errors happen when pulling the application.
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   */
  public ApplicationRecord pullAndDeploy(ApplicationReference appRef) throws Exception {
    // Deploy with a generated uuid
    String versionId = RunIds.generate().getId();
    ApplicationId appId = appRef.app(versionId);
    // Enforcing application CREATE permission and Namespace READ_REPOSITORY permission upfront
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.CREATE);
    accessEnforcer.enforce(appRef.getParent(), authenticationContext.getPrincipal(),
        NamespacePermission.READ_REPOSITORY);

    PullAppResponse<?> pullResponse = pullAndValidateApplication(appRef);

    AppRequest<?> appRequest = pullResponse.getAppRequest();
    SourceControlMeta sourceControlMeta = new SourceControlMeta(
        pullResponse.getApplicationFileHash());
    PreferencesDetail preferences = pullResponse.getPreferences();
    List<ScheduleDetail> schedules = pullResponse.getSchedules();

    LOG.info("Start to deploy app {} in namespace {} by user {}",
        appId.getApplication(),
        appId.getParent(),
        appLifecycleService.decodeUserId(authenticationContext));

    ApplicationWithPrograms app = appLifecycleService.deployApp(appId, appRequest,
        sourceControlMeta, x -> {
        });

    LOG.info(
        "Successfully deployed app {} in namespace {} from artifact {} with configuration {} and "
            + "principal {}", app.getApplicationId().getApplication(),
        app.getApplicationId().getNamespace(),
        app.getArtifactId(), appRequest.getConfig(), app.getOwnerPrincipal());

    if (preferences != null) {
      LOG.info("Updating configuration {} for app {} in namespace {} by user {}",
          preferences.getProperties(), app.getApplicationId(),
          appId.getParent(), appLifecycleService.decodeUserId(authenticationContext));

      preferencesService.setProperties(app.getApplicationId(), preferences.getProperties());
    }

    if (schedules != null) {
      LOG.info("Updating schedules {} for app {} in namespace {} by user {}",
          schedules, app.getApplicationId(),
          appId.getParent(), appLifecycleService.decodeUserId(authenticationContext));

      for (ScheduleDetail scheduleDetail : schedules) {
        try {
          ScheduleId scheduleId = appId.schedule(scheduleDetail.getName());
          try {
            programScheduleService.update(scheduleId, scheduleDetail);
          } catch (NotFoundException e) {
            ProgramType programType = ProgramType.valueOfSchedulableType(
                scheduleDetail.getProgram().getProgramType());
            String programName = scheduleDetail.getProgram().getProgramName();
            ProgramId programId = appId.program(programType, programName);
            String description = Objects.firstNonNull(scheduleDetail.getDescription(), "");
            Map<String, String> properties = Objects.firstNonNull(scheduleDetail.getProperties(),
                Collections.emptyMap());
            List<? extends Constraint> constraints = Objects.firstNonNull(
                scheduleDetail.getConstraints(), NO_CONSTRAINTS);
            long timeoutMillis =
                Objects.firstNonNull(scheduleDetail.getTimeoutMillis(),
                    Schedulers.JOB_QUEUE_TIMEOUT_MILLIS);
            ProgramSchedule schedule = new ProgramSchedule(scheduleDetail.getName(), description,
                programId, properties, scheduleDetail.getTrigger(), constraints, timeoutMillis);
            programScheduleService.add(schedule);
          }
        } catch (Exception e) {
          LOG.warn("Failed to update schedule {} : {} \n {}", scheduleDetail.getName(), e.getMessage(), e.getStackTrace());
        }
      }

    }

    return new ApplicationRecord(
        ArtifactSummary.from(app.getArtifactId().toApiArtifactId()),
        app.getApplicationId().getApplication(),
        app.getApplicationId().getVersion(),
        app.getSpecification().getDescription(),
        Optional.ofNullable(app.getOwnerPrincipal()).map(KerberosPrincipalId::getPrincipal)
            .orElse(null),
        app.getChangeDetail(), app.getSourceControlMeta());
  }

  /**
   * Pull the application from repository, look up the fileHash in store and compare it with the
   * cone in repository.
   *
   * @param appRef {@link ApplicationReference} to fetch the application with
   * @return {@link PullAppResponse}
   * @throws NoChangesToPullException if the fileHashes are the same
   * @throws NotFoundException if the repository config is not found or the application in
   *     repository is not found
   * @throws SourceControlException if unexpected errors happen when pulling the application.
   * @throws AuthenticationConfigException if the repository configuration authentication fails
   */
  private PullAppResponse<?> pullAndValidateApplication(ApplicationReference appRef)
      throws NoChangesToPullException, NotFoundException, AuthenticationConfigException {
    RepositoryConfig repoConfig = getRepositoryMeta(appRef.getParent()).getConfig();
    SourceControlMeta latestMeta = store.getAppSourceControlMeta(appRef);
    PullAppResponse<?> pullResponse = sourceControlOperationRunner.pull(
        new PulAppOperationRequest(appRef, repoConfig));

    // if (latestMeta != null
    //     && latestMeta.getFileHash().equals(pullResponse.getApplicationFileHash())) {
    //   throw new NoChangesToPullException(String.format("Pipeline deployment was not successful because there is "
    //       + "no new change for the pulled application: %s", appRef));
    // }
    return pullResponse;
  }

  /**
   * The method to list all applications found in linked repository.
   *
   * @return {@link RepositoryAppsResponse}
   * @throws RepositoryNotFoundException if the repository config is not found
   * @throws AuthenticationConfigException if git auth config is not found
   * @throws SourceControlException if {@link SourceControlOperationRunner} fails to list
   *     applications
   */
  public RepositoryAppsResponse listApps(NamespaceId namespace) throws NotFoundException,
      AuthenticationConfigException {
    accessEnforcer.enforce(namespace, authenticationContext.getPrincipal(),
        NamespacePermission.READ_REPOSITORY);
    RepositoryConfig repoConfig = getRepositoryMeta(namespace).getConfig();
    return sourceControlOperationRunner.list(new NamespaceRepository(namespace, repoConfig));
  }

}
