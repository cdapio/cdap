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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ApplicationUpdateResult;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.CannotBeDeletedException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.NotImplementedException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.internal.app.DefaultApplicationUpdateContext;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationUpdateDetails;
import io.cdap.cdap.proto.DatasetDetail;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProgramTypes;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.spi.metadata.MetadataMutation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Service that manage lifecycle of Applications.
 */
public class ApplicationLifecycleService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationLifecycleService.class);
  private static final Gson GSON =
      new GsonBuilder().registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();

  /**
   * Store manages non-runtime lifecycle.
   */
  private final Store store;
  private final Scheduler scheduler;
  private final UsageRegistry usageRegistry;
  private final PreferencesService preferencesService;
  private final MetricsSystemClient metricsSystemClient;
  private final OwnerAdmin ownerAdmin;
  private final ArtifactRepository artifactRepository;
  private final ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final MetadataServiceClient metadataServiceClient;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final boolean appUpdateSchedules;
  private final AdminEventPublisher adminEventPublisher;
  private final Impersonator impersonator;

  @Inject
  ApplicationLifecycleService(CConfiguration cConfiguration,
                              Store store, Scheduler scheduler, UsageRegistry usageRegistry,
                              PreferencesService preferencesService, MetricsSystemClient metricsSystemClient,
                              OwnerAdmin ownerAdmin, ArtifactRepository artifactRepository,
                              ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
                              MetadataServiceClient metadataServiceClient,
                              AuthorizationEnforcer authorizationEnforcer, AuthenticationContext authenticationContext,
                              MessagingService messagingService, Impersonator impersonator) {
    this.appUpdateSchedules = cConfiguration.getBoolean(Constants.AppFabric.APP_UPDATE_SCHEDULES,
                                                        Constants.AppFabric.DEFAULT_APP_UPDATE_SCHEDULES);
    this.store = store;
    this.scheduler = scheduler;
    this.usageRegistry = usageRegistry;
    this.preferencesService = preferencesService;
    this.metricsSystemClient = metricsSystemClient;
    this.artifactRepository = artifactRepository;
    this.managerFactory = managerFactory;
    this.metadataServiceClient = metadataServiceClient;
    this.ownerAdmin = ownerAdmin;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authenticationContext = authenticationContext;
    this.impersonator = impersonator;
    this.adminEventPublisher = new AdminEventPublisher(cConfiguration,
                                                       new MultiThreadMessagingContext(messagingService));
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting ApplicationLifecycleService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down ApplicationLifecycleService");
  }

  /**
   * Get all applications in the specified namespace, filtered to only include applications with an artifact name
   * in the set of specified names and an artifact version equal to the specified version. If the specified set
   * is empty, no filtering is performed on artifact name. If the specified version is null, no filtering is done
   * on artifact version.
   *
   * @param namespace the namespace to get apps from
   * @param artifactNames the set of valid artifact names. If empty, all artifact names are valid
   * @param artifactVersion the artifact version to match. If null, all artifact versions are valid
   * @return list of all applications in the namespace that match the specified artifact names and version
   */
  public List<ApplicationDetail> getApps(NamespaceId namespace, Set<String> artifactNames,
                                         @Nullable String artifactVersion) throws Exception {
    return getApps(namespace, getAppPredicate(artifactNames, artifactVersion));
  }

  /**
   * Get all applications in the specified namespace that satisfy the specified predicate.
   *
   * @param namespace the namespace to get apps from
   * @param predicate the predicate that must be satisfied in order to be returned
   * @return list of all applications in the namespace that satisfy the specified predicate
   */
  public List<ApplicationDetail> getApps(NamespaceId namespace,
                                         Predicate<ApplicationDetail> predicate) throws Exception {

    Map<ApplicationId, ApplicationSpecification> appSpecs = new LinkedHashMap<>();
    for (ApplicationSpecification appSpec : store.getAllApplications(namespace)) {
      appSpecs.put(namespace.app(appSpec.getName(), appSpec.getAppVersion()), appSpec);
    }
    Set<? extends EntityId> visible = authorizationEnforcer.isVisible(appSpecs.keySet(),
                                                                      authenticationContext.getPrincipal());
    appSpecs.keySet().removeIf(id -> !visible.contains(id));

    Map<ApplicationId, String> owners = ownerAdmin.getOwnerPrincipals(appSpecs.keySet());
    List<ApplicationDetail> result = new ArrayList<>();
    for (Map.Entry<ApplicationId, ApplicationSpecification> entry : appSpecs.entrySet()) {
      ApplicationDetail applicationDetail = ApplicationDetail.fromSpec(entry.getValue(), owners.get(entry.getKey()));
      if (predicate.test(applicationDetail)) {
        result.add(filterApplicationDetail(entry.getKey(), applicationDetail));
      }
    }
    return result;
  }

  /**
   * Get detail about the specified application
   *
   * @param appId the id of the application to get
   * @return detail about the specified application
   * @throws ApplicationNotFoundException if the specified application does not exist
   */
  public ApplicationDetail getAppDetail(ApplicationId appId) throws Exception {
    // TODO: CDAP-12473: filter based on the entity visibility in the app detail
    // user needs to pass the visibility check to get the app detail
    AuthorizationUtil.ensureAccess(appId, authorizationEnforcer, authenticationContext.getPrincipal());
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }
    String ownerPrincipal = ownerAdmin.getOwnerPrincipal(appId);
    return filterApplicationDetail(appId, ApplicationDetail.fromSpec(appSpec, ownerPrincipal));
  }

  /**
   * Gets details for a set of given applications.
   *
   * @param appIds the set of application id to get details
   * @return a {@link Map} from the application id to the corresponding detail. There will be no entry for applications
   * that don't exist.
   * @throws Exception if failed to get details.
   */
  public Map<ApplicationId, ApplicationDetail> getAppDetails(Collection<ApplicationId> appIds) throws Exception {
    Set<? extends EntityId> visibleIds = authorizationEnforcer.isVisible(new HashSet<>(appIds),
                                                                         authenticationContext.getPrincipal());
    Set<ApplicationId> filterIds = appIds.stream().filter(visibleIds::contains).collect(Collectors.toSet());
    Map<ApplicationId, ApplicationSpecification> appSpecs = store.getApplications(filterIds);
    Map<ApplicationId, String> principals = ownerAdmin.getOwnerPrincipals(filterIds);

    Map<ApplicationId, ApplicationDetail> result = new HashMap<>();
    for (Map.Entry<ApplicationId, ApplicationSpecification> entry : appSpecs.entrySet()) {
      ApplicationId appId = entry.getKey();
      result.put(appId,
                 filterApplicationDetail(appId, ApplicationDetail.fromSpec(entry.getValue(), principals.get(appId))));
    }
    return result;
  }

  public Collection<String> getAppVersions(String namespace, String application) throws Exception {
    Collection<ApplicationId> appIds = store.getAllAppVersionsAppIds(new ApplicationId(namespace, application));
    List<String> versions = new ArrayList<>();
    for (ApplicationId appId : appIds) {
      versions.add(appId.getVersion());
    }
    return versions;
  }

  /**
   * To determine whether the app version is allowed to be deployed
   *
   * @param appId the id of the application to be determined
   * @return whether the app version is allowed to be deployed
   */
  public boolean updateAppAllowed(ApplicationId appId) throws Exception {
    AuthorizationUtil.ensureAccess(appId, authorizationEnforcer, authenticationContext.getPrincipal());
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      // App does not exist. Allow to create a new one
      return true;
    }
    String version = appId.getVersion();
    return version.endsWith(ApplicationId.DEFAULT_VERSION);
  }

  /**
   * Update an existing application. An application's configuration and artifact version can be updated.
   *
   * @param appId the id of the application to update
   * @param appRequest the request to update the application, including new config and artifact
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return information about the deployed application
   * @throws ApplicationNotFoundException if the specified application does not exist
   * @throws ArtifactNotFoundException if the requested artifact does not exist
   * @throws InvalidArtifactException if the specified artifact is invalid. For example, if the artifact name changed,
   *                                  if the version is an invalid version, or the artifact contains no app classes
   * @throws Exception if there was an exception during the deployment pipeline. This exception will often wrap
   *                   the actual exception
   */
  public ApplicationWithPrograms updateApp(ApplicationId appId, AppRequest appRequest,
                                           ProgramTerminator programTerminator) throws Exception {
    // Check if the current user has admin privileges on it before updating.
    authorizationEnforcer.enforce(appId, authenticationContext.getPrincipal(), Action.ADMIN);

    // check that app exists
    ApplicationSpecification currentSpec = store.getApplication(appId);
    if (currentSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }

    ArtifactId currentArtifact = currentSpec.getArtifactId();

    // if no artifact is given, use the current one.
    ArtifactId newArtifactId = currentArtifact;
    // otherwise, check requested artifact is valid and use it
    ArtifactSummary requestedArtifact = appRequest.getArtifact();
    if (requestedArtifact != null) {
      // cannot change artifact name, only artifact version.
      if (!currentArtifact.getName().equals(requestedArtifact.getName())) {
        throw new InvalidArtifactException(String.format(
          " Only artifact version updates are allowed. Cannot change from artifact '%s' to '%s'.",
          currentArtifact.getName(), requestedArtifact.getName()));
      }

      if (!currentArtifact.getScope().equals(requestedArtifact.getScope())) {
        throw new InvalidArtifactException("Only artifact version updates are allowed. " +
          "Cannot change from a non-system artifact to a system artifact or vice versa.");
      }

      // check requested artifact version is valid
      ArtifactVersion requestedVersion = new ArtifactVersion(requestedArtifact.getVersion());
      if (requestedVersion.getVersion() == null) {
        throw new InvalidArtifactException(String.format(
          "Requested artifact version '%s' is invalid", requestedArtifact.getVersion()));
      }
      newArtifactId = new ArtifactId(currentArtifact.getName(), requestedVersion, currentArtifact.getScope());
    }

    // ownerAdmin.getImpersonationPrincipal will give the owner which will be impersonated for the application
    // irrespective of the version
    SecurityUtil.verifyOwnerPrincipal(appId, appRequest.getOwnerPrincipal(), ownerAdmin);

    Object requestedConfigObj = appRequest.getConfig();
    // if config is null, use the previous config. Shouldn't use a static GSON since the request Config object can
    // be a user class, otherwise there will be ClassLoader leakage.
    String requestedConfigStr = requestedConfigObj == null ?
      currentSpec.getConfiguration() : new Gson().toJson(requestedConfigObj);

    Id.Artifact artifactId = Id.Artifact.fromEntityId(Artifacts.toProtoArtifactId(appId.getParent(), newArtifactId));
    return deployApp(appId.getParent(), appId.getApplication(), null, artifactId, requestedConfigStr,
                     programTerminator, ownerAdmin.getOwner(appId), appRequest.canUpdateSchedules());
  }

  /**
   * Upgrades an existing application by upgrading appliation artifact versions and plugin artifact versions.
   *
   * @param appId the id of the application to upgrade.
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return Details of upgrade for the application like failure details with reason.
   * @return
   * @throws Exception
   */
  public ApplicationUpdateDetails upgradeApplication(ApplicationId appId, ProgramTerminator programTerminator)
      throws Exception {
    // Check if the current user has admin privileges on it before updating.
    authorizationEnforcer.enforce(appId, authenticationContext.getPrincipal(), Action.ADMIN);
    // check that app exists
    ApplicationSpecification currentSpec = store.getApplication(appId);
    if (currentSpec == null) {
      LOG.warn("Application {} not found for upgrade.", appId);
      return new ApplicationUpdateDetails(new NotFoundException(appId));
    }
    ArtifactId currentArtifact = currentSpec.getArtifactId();
    NamespaceId currentArtifactNamespace =
        ArtifactScope.SYSTEM.equals(currentArtifact.getScope()) ? NamespaceId.SYSTEM : appId.getParent();

    // Get the artifact with latest version for upgrade.
    List<ArtifactSummary> availableArtifacts =
        artifactRepository.getArtifactSummaries(currentArtifactNamespace, currentArtifact.getName(), Integer.MAX_VALUE,
                                                ArtifactSortOrder.ASC);
    if (availableArtifacts.isEmpty()) {
      // This should not be possible as at least the current used artifact should be there.
      LOG.error("No artifacts found for artifact {} ", currentArtifact);
      throw new InternalError("Issues in trying to find application artifact for upgrading app " + appId);
    }
    ArtifactSummary candidateArtifact = availableArtifacts.get(availableArtifacts.size() - 1);
    ArtifactVersion candidateArtifactVersion = new ArtifactVersion(candidateArtifact.getVersion());
    if (candidateArtifactVersion.getVersion() == null) {
      throw new InvalidArtifactException(String.format(
          "Requested artifact version '%s' is invalid", candidateArtifact.getVersion()));
    }

    // Check conditions for validity of candidate artifact such as name and scope should be same. Also check that
    // candidate artifact should have higher version than current artifact.
    // In ideal cases, these conditions should never happen.
    if (!currentArtifact.getName().equals(candidateArtifact.getName())) {
      throw new InvalidArtifactException(String.format(
          "Requested artifact name %s does not match with current name %s.", candidateArtifact.getName(),
          currentArtifact.getName()));
    }
    if (currentArtifact.getScope() != candidateArtifact.getScope()) {
      throw new InvalidArtifactException(String.format(
          "Requested artifact scope %s does not match with current name %s.", candidateArtifact.getScope(),
          currentArtifact.getScope()));
    }
    if (currentArtifact.getVersion().compareTo(candidateArtifactVersion) > 0) {
      throw new InvalidArtifactException(String.format(
          "Requested artifact version %s is older than current artifact version %s.", candidateArtifact.getVersion(),
          currentArtifact.getVersion()));
    }

    ArtifactId newArtifactId =
        new ArtifactId(currentArtifact.getName(), candidateArtifactVersion, currentArtifact.getScope());

    Id.Artifact newArtifact = Id.Artifact.fromEntityId(Artifacts.toProtoArtifactId(appId.getParent(), newArtifactId));
    ArtifactDetail newArtifactDetail = artifactRepository.getArtifact(newArtifact);
    List<ApplicationConfigUpdateAction> upgradeActions = Arrays.asList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT);

    try {
      ApplicationUpdateDetails detail =
          updateApplicationInternal(appId, appId.getParent(), appId.getApplication(), null,
                                    currentSpec.getConfiguration(), programTerminator, newArtifactDetail,
                                    upgradeActions, ownerAdmin.getOwner(appId), /*updateSchedules=*/false);
      LOG.debug("Application upgrade successful. Update details: {}. Error: {}", detail.getUpdateDetails(),
                detail.getError());
      return new ApplicationUpdateDetails("upgrade successful.", null);
    } catch (UnsupportedOperationException E) {
      String errorMessage = String.format("Upgrade failed for application %s as artifact %s does not support upgrade.",
                                          appId, newArtifact);
      return new ApplicationUpdateDetails(new NotImplementedException(errorMessage));
    }
  }

  // Updates an application config by applying given update actions. The app should know how to apply these actions
  // to its config.
  private ApplicationUpdateDetails updateApplicationInternal(ApplicationId applicationId,
                                                             NamespaceId namespaceId,
                                                             @Nullable String appName,
                                                             @Nullable String appVersion,
                                                             @Nullable String currentConfigStr,
                                                             ProgramTerminator programTerminator,
                                                             ArtifactDetail artifactDetail,
                                                             List<ApplicationConfigUpdateAction> updateActions,
                                                             @Nullable KerberosPrincipalId ownerPrincipal,
                                                             boolean updateSchedules) throws Exception {
    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null);
    if (appClass == null) {
      throw new InvalidArtifactException(String.format("No application class found in artifact '%s' in namespace '%s'.",
                                         artifactDetail.getDescriptor().getArtifactId(), namespaceId));
    }
    io.cdap.cdap.proto.id.ArtifactId artifactId =
        Artifacts.toProtoArtifactId(namespaceId, artifactDetail.getDescriptor().getArtifactId());
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId, this.impersonator);
    ClassLoader artifactClassLoader =
        artifactRepository.createArtifactClassLoader(artifactDetail.getDescriptor().getLocation(),
                                                     classLoaderImpersonator);

    String updatedAppConfig = "";
    DefaultApplicationUpdateContext updateContext =
        new DefaultApplicationUpdateContext(namespaceId, applicationId, artifactDetail.getDescriptor().getArtifactId(),
                                            artifactRepository, currentConfigStr, updateActions);

    // Run config update logic for the application to generate updated config.
    try {
      Object appMain = artifactClassLoader.loadClass(appClass.getClassName()).newInstance();
      if (!(appMain instanceof Application)) {
        throw new IllegalStateException(
            String.format("Application main class is of invalid type: %s",
                          appMain.getClass().getName()));
      }
      Application app = (Application) appMain;
      Type configType = Artifacts.getConfigType(app.getClass());
      try {
        ApplicationUpdateResult<?> updateResult = app.updateConfig(updateContext);
        updatedAppConfig = GSON.toJson(updateResult.getNewConfig(), configType);
      } catch (UnsupportedOperationException ex) {
        String errorMessage = String.format("application of type %s does not support update.",
                                            appMain.getClass().getName());
        LOG.error("Application update failed due to " + errorMessage);
        throw ex;
      }
    } catch (Exception ex) {
      Throwables.propagateIfPossible(ex.getCause(), Exception.class);
      throw Throwables.propagate(ex.getCause());
    }

    // Deploy application with with potentially new app config and new artifact.
    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactDetail.getDescriptor(), namespaceId,
                                                             appClass.getClassName(), appName, appVersion,
                                                             updatedAppConfig, ownerPrincipal,
                                                             updateSchedules);

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(programTerminator);
    // TODO: (CDAP-3258) Manager needs MUCH better error handling.
    ApplicationWithPrograms applicationWithPrograms;
    try {
      applicationWithPrograms = manager.deploy(deploymentInfo).get();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), Exception.class);
      throw Throwables.propagate(e.getCause());
    }
    adminEventPublisher.publishAppCreation(applicationWithPrograms.getApplicationId(),
                                           applicationWithPrograms.getSpecification());
    return new ApplicationUpdateDetails("Update successful.", null);
  }

  /**
   * Deploy an application by first adding the application jar to the artifact repository, then creating an application
   * using that newly added artifact.
   *
   * @param namespace the namespace to deploy the application and artifact in
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param artifactId the id of the artifact to add and create the application from
   * @param jarFile the application jar to add as an artifact and create the application from
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return information about the deployed application
   * @throws InvalidArtifactException the the artifact is invalid. For example, if it does not contain any app classes
   * @throws ArtifactAlreadyExistsException if the specified artifact already exists
   * @throws IOException if there was an IO error writing the artifact
   */
  public ApplicationWithPrograms deployAppAndArtifact(NamespaceId namespace, @Nullable String appName,
                                                      Id.Artifact artifactId, File jarFile,
                                                      @Nullable String configStr,
                                                      @Nullable KerberosPrincipalId ownerPrincipal,
                                                      ProgramTerminator programTerminator,
                                                      boolean updateSchedules) throws Exception {

    ArtifactDetail artifactDetail = artifactRepository.addArtifact(artifactId, jarFile);
    try {
      return deployApp(namespace, appName, null, configStr, programTerminator, artifactDetail, ownerPrincipal,
                       updateSchedules);
    } catch (Exception e) {
      // if we added the artifact, but failed to deploy the application, delete the artifact to bring us back
      // to the state we were in before this call.
      try {
        artifactRepository.deleteArtifact(artifactId);
      } catch (IOException e2) {
        // if the delete fails, nothing we can do, just log it and continue on
        LOG.warn("Failed to delete artifact {} after deployment of artifact and application failed.", artifactId, e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed, the Application
   * class is instantiated and configure() is called in order to generate an {@link ApplicationSpecification}.
   * Programs, datasets, and streams are created based on the specification before the spec is persisted in the
   * {@link Store}. This method can create a new application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param artifactId the id of the artifact to create the application from
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws Exception if there was an exception during the deployment pipeline. This exception will often wrap
   *                   the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace, @Nullable String appName, @Nullable String appVersion,
                                           Id.Artifact artifactId,
                                           @Nullable String configStr,
                                           ProgramTerminator programTerminator) throws Exception {
    return deployApp(namespace, appName, appVersion, artifactId, configStr, programTerminator, null, true);
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed, the Application
   * class is instantiated and configure() is called in order to generate an {@link ApplicationSpecification}.
   * Programs, datasets, and streams are created based on the specification before the spec is persisted in the
   * {@link Store}. This method can create a new application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param artifactId the id of the artifact to create the application from
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @param ownerPrincipal the kerberos principal of the application owner
   * @param updateSchedules specifies if schedules of the workflow have to be updated,
   *                        if null value specified by the property "app.deploy.update.schedules" will be used.
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws Exception if there was an exception during the deployment pipeline. This exception will often wrap
   *                   the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace, @Nullable String appName, @Nullable String appVersion,
                                           Id.Artifact artifactId,
                                           @Nullable String configStr,
                                           ProgramTerminator programTerminator,
                                           @Nullable KerberosPrincipalId ownerPrincipal,
                                           @Nullable Boolean updateSchedules) throws Exception {
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    return deployApp(namespace, appName, appVersion, configStr, programTerminator, artifactDetail, ownerPrincipal,
                     updateSchedules == null ? appUpdateSchedules : updateSchedules);
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed, the Application
   * class is instantiated and configure() is called in order to generate an {@link ApplicationSpecification}.
   * Programs, datasets, and streams are created based on the specification before the spec is persisted in the
   * {@link Store}. This method can create a new application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application spec
   * @param summary the artifact summary of the app
   * @param configStr the configuration to send to the application when generating the application specification
   * @param programTerminator a program terminator that will stop programs that are removed when updating an app.
   *                          For example, if an update removes a flow, the terminator defines how to stop that flow.
   * @param ownerPrincipal the kerberos principal of the application owner
   * @param updateSchedules specifies if schedules of the workflow have to be updated,
   *                        if null value specified by the property "app.deploy.update.schedules" will be used.
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws Exception if there was an exception during the deployment pipeline. This exception will often wrap
   *                   the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace, @Nullable String appName, @Nullable String appVersion,
                                           ArtifactSummary summary,
                                           @Nullable String configStr,
                                           ProgramTerminator programTerminator,
                                           @Nullable KerberosPrincipalId ownerPrincipal,
                                           @Nullable Boolean updateSchedules) throws Exception {
    NamespaceId artifactNamespace =
      ArtifactScope.SYSTEM.equals(summary.getScope()) ? NamespaceId.SYSTEM : namespace;
    ArtifactRange range = new ArtifactRange(artifactNamespace.getNamespace(), summary.getName(),
                                            ArtifactVersionRange.parse(summary.getVersion()));
    // this method will not throw ArtifactNotFoundException, if no artifacts in the range, we are expecting an empty
    // collection returned.
    List<ArtifactDetail> artifactDetail = artifactRepository.getArtifactDetails(range, 1, ArtifactSortOrder.DESC);
    if (artifactDetail.isEmpty()) {
      throw new ArtifactNotFoundException(range.getNamespace(), range.getName());
    }
    return deployApp(namespace, appName, appVersion, configStr, programTerminator, artifactDetail.iterator().next(),
                     ownerPrincipal, updateSchedules == null ? appUpdateSchedules : updateSchedules);
  }

  /**
   * Remove all the applications inside the given {@link Id.Namespace}
   *
   * @param namespaceId the {@link NamespaceId} under which all application should be deleted
   * @throws Exception
   */
  public void removeAll(NamespaceId namespaceId) throws Exception {
    Map<ProgramRunId, RunRecordDetail> runningPrograms = store.getActiveRuns(namespaceId);
    List<ApplicationSpecification> allSpecs = new ArrayList<>(store.getAllApplications(namespaceId));
    Map<ApplicationId, ApplicationSpecification> apps = new HashMap<>();
    for (ApplicationSpecification appSpec : allSpecs) {
      ApplicationId applicationId = namespaceId.app(appSpec.getName(), appSpec.getAppVersion());
      authorizationEnforcer.enforce(applicationId, authenticationContext.getPrincipal(), Action.ADMIN);
      apps.put(applicationId, appSpec);
    }

    if (!runningPrograms.isEmpty()) {
      Set<String> activePrograms = new HashSet<>();
      for (Map.Entry<ProgramRunId, RunRecordDetail> runningProgram : runningPrograms.entrySet()) {
        activePrograms.add(runningProgram.getKey().getApplication() +
                            ": " + runningProgram.getKey().getProgram());
      }

      String appAllRunningPrograms = Joiner.on(',')
        .join(activePrograms);
      throw new CannotBeDeletedException(namespaceId,
                                         "The following programs are still running: " + appAllRunningPrograms);
    }

    // All Apps are STOPPED, delete them
    for (ApplicationId appId : apps.keySet()) {
      removeAppInternal(appId, apps.get(appId));
    }
  }

  /**
   * Delete an application specified by appId.
   *
   * @param appId the {@link ApplicationId} of the application to be removed
   * @throws Exception
   */
  public void removeApplication(ApplicationId appId) throws Exception {
    // enforce ADMIN privileges on the app
    authorizationEnforcer.enforce(appId, authenticationContext.getPrincipal(), Action.ADMIN);
    ensureNoRunningPrograms(appId);
    ApplicationSpecification spec = store.getApplication(appId);
    if (spec == null) {
      throw new NotFoundException(Id.Application.fromEntityId(appId));
    }

    removeAppInternal(appId, spec);
  }

  /**
   * Remove application by the appId and appSpec, note that this method does not have any auth check
   *
   * @param applicationId the {@link ApplicationId} of the application to be removed
   * @param appSpec the {@link ApplicationSpecification} of the application to be removed
   */
  private void removeAppInternal(ApplicationId applicationId, ApplicationSpecification appSpec) throws Exception {
    // if the application has only one version, do full deletion, else only delete the specified version
    if (store.getAllAppVersions(applicationId).size() == 1) {
      deleteApp(applicationId, appSpec);
      return;
    }
    deleteAppVersion(applicationId, appSpec);
  }

  /**
   * Find if the given application has running programs
   *
   * @param appId the id of the application to find running programs for
   * @throws CannotBeDeletedException : the application cannot be deleted because of running programs
   */
  private void ensureNoRunningPrograms(ApplicationId appId) throws CannotBeDeletedException {
    //Check if all are stopped.
    Map<ProgramRunId, RunRecordDetail> runningPrograms = store.getActiveRuns(appId);

    if (!runningPrograms.isEmpty()) {
      Set<String> activePrograms = new HashSet<>();
      for (Map.Entry<ProgramRunId, RunRecordDetail> runningProgram : runningPrograms.entrySet()) {
        activePrograms.add(runningProgram.getKey().getProgram());
      }

      String appAllRunningPrograms = Joiner.on(',')
        .join(activePrograms);
      throw new CannotBeDeletedException(appId,
                                         "The following programs are still running: " + appAllRunningPrograms);
    }
  }

  /**
   * Get detail about the plugin in the specified application
   *
   * @param appId the id of the application
   * @return list of plugins in the application
   * @throws ApplicationNotFoundException if the specified application does not exist
   */
  public List<PluginInstanceDetail> getPlugins(ApplicationId appId)
    throws ApplicationNotFoundException {
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }
    List<PluginInstanceDetail> pluginInstanceDetails = new ArrayList<>();
    for (Map.Entry<String, Plugin> entry : appSpec.getPlugins().entrySet()) {
      pluginInstanceDetails.add(new PluginInstanceDetail(entry.getKey(), entry.getValue()));
    }
    return pluginInstanceDetails;
  }

  private Iterable<ProgramSpecification> getProgramSpecs(ApplicationId appId) {
    ApplicationSpecification appSpec = store.getApplication(appId);
    return Iterables.concat(appSpec.getMapReduce().values(),
                            appSpec.getServices().values(),
                            appSpec.getSpark().values(),
                            appSpec.getWorkers().values(),
                            appSpec.getWorkflows().values());
  }

  /**
   * Delete the metrics for an application.
   *
   * @param applicationId the application to delete metrics for.
   */
  private void deleteMetrics(ApplicationId applicationId) throws IOException {
    ApplicationSpecification spec = this.store.getApplication(applicationId);
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, applicationId.getNamespace());
    // add or replace application name in the tagMap
    tags.put(Constants.Metrics.Tag.APP, spec.getName());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.emptySet(), tags,
                                                          new ArrayList<>(tags.keySet()));
    metricsSystemClient.delete(deleteQuery);
  }

  /**
   * Delete stored Preferences of the application and all its programs.
   *
   * @param appId applicationId
   */
  private void deletePreferences(ApplicationId appId) {
    Iterable<ProgramSpecification> programSpecs = getProgramSpecs(appId);
    for (ProgramSpecification spec : programSpecs) {

      preferencesService.deleteProperties(appId.program(ProgramTypes.fromSpecification(spec), spec.getName()));
      LOG.trace("Deleted Preferences of Program : {}, {}, {}, {}", appId.getNamespace(), appId.getApplication(),
                ProgramTypes.fromSpecification(spec).getCategoryName(), spec.getName());
    }
    preferencesService.deleteProperties(appId);
    LOG.trace("Deleted Preferences of Application : {}, {}", appId.getNamespace(), appId.getApplication());
  }

  private ApplicationWithPrograms deployApp(NamespaceId namespaceId, @Nullable String appName,
                                            @Nullable String appVersion,
                                            @Nullable String configStr,
                                            ProgramTerminator programTerminator,
                                            ArtifactDetail artifactDetail,
                                            @Nullable KerberosPrincipalId ownerPrincipal,
                                            boolean updateSchedules) throws Exception {
    // Now to deploy an app, we need ADMIN privilege on the owner principal if it is present, and also ADMIN on the app
    // But since at this point, app name is unknown to us, so the enforcement on the app is happening in the deploy
    // pipeline - LocalArtifactLoaderStage

    // need to enforce on the principal id if impersonation is involved
    KerberosPrincipalId effectiveOwner =
      SecurityUtil.getEffectiveOwner(ownerAdmin, namespaceId,
                                     ownerPrincipal == null ? null : ownerPrincipal.getPrincipal());


    Principal requestingUser = authenticationContext.getPrincipal();
    // enforce that the current principal, if not the same as the owner principal, has the admin privilege on the
    // impersonated principal
    if (effectiveOwner != null) {
      authorizationEnforcer.enforce(effectiveOwner, requestingUser, Action.ADMIN);
    }

    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(), null);
    if (appClass == null) {
      throw new InvalidArtifactException(String.format("No application class found in artifact '%s' in namespace '%s'.",
                                                       artifactDetail.getDescriptor().getArtifactId(), namespaceId));
    }

    // deploy application with newly added artifact
    AppDeploymentInfo deploymentInfo = new AppDeploymentInfo(artifactDetail.getDescriptor(), namespaceId,
                                                             appClass.getClassName(), appName, appVersion,
                                                             configStr, ownerPrincipal, updateSchedules);

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(programTerminator);
    // TODO: (CDAP-3258) Manager needs MUCH better error handling.
    ApplicationWithPrograms applicationWithPrograms;
    try {
      applicationWithPrograms = manager.deploy(deploymentInfo).get();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), Exception.class);
      throw Throwables.propagate(e.getCause());
    }

    adminEventPublisher.publishAppCreation(applicationWithPrograms.getApplicationId(),
                                           applicationWithPrograms.getSpecification());
    return applicationWithPrograms;
  }

  // deletes without performs checks that no programs are running

  /**
   * Delete the specified application without performing checks that its programs are stopped.
   *
   * @param appId the id of the application to delete
   * @param spec the spec of the application to delete
   * @throws Exception
   */
  private void deleteApp(ApplicationId appId, ApplicationSpecification spec) throws Exception {
    //Delete the schedules
    scheduler.deleteSchedules(appId);
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      scheduler.modifySchedulesTriggeredByDeletedProgram(appId.workflow(workflowSpec.getName()));
    }

    deleteMetrics(appId);

    //Delete all preferences of the application and of all its programs
    deletePreferences(appId);

    ApplicationSpecification appSpec = store.getApplication(appId);
    deleteAppMetadata(appId, appSpec);
    store.deleteWorkflowStats(appId);
    store.removeApplication(appId);
    try {
      // delete the owner as it has already been determined that this is the only version of the app
      ownerAdmin.delete(appId);
    } catch (Exception e) {
      LOG.warn("Failed to delete app owner principal for application {} if one existed while deleting the " +
                 "application.", appId);
    }

    try {
      usageRegistry.unregister(appId);
    } catch (Exception e) {
      LOG.warn("Failed to unregister usage of app: {}", appId, e);
    }

    // make sure the program profile metadata is removed
    adminEventPublisher.publishAppDeletion(appId, appSpec);
  }

  /**
   * Delete the specified application version without performing checks that its programs are stopped.
   *
   * @param appId the id of the application to delete
   * @param spec the spec of the application to delete
   */
  private void deleteAppVersion(ApplicationId appId, ApplicationSpecification spec) {
    //Delete the schedules
    scheduler.deleteSchedules(appId);
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      scheduler.modifySchedulesTriggeredByDeletedProgram(appId.workflow(workflowSpec.getName()));
    }
    store.removeApplication(appId);
  }

  /**
   * Delete the metadata for the application and the programs.
   */
  private void deleteAppMetadata(ApplicationId appId, ApplicationSpecification appSpec) {
    // Remove metadata for the Application itself.
    metadataServiceClient.drop(new MetadataMutation.Drop(appId.toMetadataEntity()));

    // Remove metadata for the programs of the Application
    // TODO: Need to remove this we support prefix search of metadata type.
    // See https://issues.cask.co/browse/CDAP-3669
    for (ProgramId programId : getAllPrograms(appId, appSpec)) {
      metadataServiceClient.drop(new MetadataMutation.Drop(programId.toMetadataEntity()));
    }
  }

  private Set<ProgramId> getProgramsWithType(ApplicationId appId, ProgramType type,
                                             Map<String, ? extends ProgramSpecification> programSpecs) {
    Set<ProgramId> result = new HashSet<>();

    for (String programName : programSpecs.keySet()) {
      result.add(appId.program(type, programName));
    }
    return result;
  }

  private Set<ProgramId> getAllPrograms(ApplicationId appId, ApplicationSpecification appSpec) {
    Set<ProgramId> result = new HashSet<>();
    result.addAll(getProgramsWithType(appId, ProgramType.MAPREDUCE, appSpec.getMapReduce()));
    result.addAll(getProgramsWithType(appId, ProgramType.WORKFLOW, appSpec.getWorkflows()));
    result.addAll(getProgramsWithType(appId, ProgramType.SERVICE, appSpec.getServices()));
    result.addAll(getProgramsWithType(appId, ProgramType.SPARK, appSpec.getSpark()));
    result.addAll(getProgramsWithType(appId, ProgramType.WORKER, appSpec.getWorkers()));
    return result;
  }

  /**
   * Filter the {@link ApplicationDetail} by only returning the visible entities
   */
  private ApplicationDetail filterApplicationDetail(ApplicationId appId,
                                                    ApplicationDetail applicationDetail) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    List<ProgramRecord> filteredPrograms =
      AuthorizationUtil.isVisible(applicationDetail.getPrograms(), authorizationEnforcer, principal,
                                  new Function<ProgramRecord, EntityId>() {
                                    @Override
                                    public EntityId apply(ProgramRecord input) {
                                      return appId.program(input.getType(), input.getName());
                                    }
                                  }, null);
    List<DatasetDetail> filteredDatasets =
      AuthorizationUtil.isVisible(applicationDetail.getDatasets(), authorizationEnforcer, principal,
                                  new Function<DatasetDetail, EntityId>() {
                                    @Override
                                    public EntityId apply(DatasetDetail input) {
                                      return appId.getNamespaceId().dataset(input.getName());
                                    }
                                  }, null);
    return new ApplicationDetail(applicationDetail.getName(), applicationDetail.getAppVersion(),
                                 applicationDetail.getDescription(), applicationDetail.getConfiguration(),
                                 filteredDatasets, filteredPrograms, applicationDetail.getPlugins(),
                                 applicationDetail.getArtifact(), applicationDetail.getOwnerPrincipal());
  }

  // get filter for app specs by artifact name and version. if they are null, it means don't filter.
  private Predicate<ApplicationDetail> getAppPredicate(Set<String> artifactNames, @Nullable String artifactVersion) {
    if (artifactNames.isEmpty() && artifactVersion == null) {
      return r -> true;
    } else if (artifactNames.isEmpty()) {
      return new ArtifactVersionPredicate(artifactVersion);
    } else if (artifactVersion == null) {
      return new ArtifactNamesPredicate(artifactNames);
    } else {
      return new ArtifactNamesPredicate(artifactNames).and(new ArtifactVersionPredicate(artifactVersion));
    }
  }

  /**
   * Returns true if the application artifact is in a whitelist of names
   */
  private static class ArtifactNamesPredicate implements Predicate<ApplicationDetail> {
    private final Set<String> names;

    ArtifactNamesPredicate(Set<String> names) {
      this.names = names;
    }

    @Override
    public boolean test(ApplicationDetail input) {
      return names.contains(input.getArtifact().getName());
    }
  }

  /**
   * Returns true if the application artifact is a specific version
   */
  private static class ArtifactVersionPredicate implements Predicate<ApplicationDetail> {
    private final String version;

    ArtifactVersionPredicate(String version) {
      this.version = version;
    }

    @Override
    public boolean test(ApplicationDetail input) {
      return version.equals(input.getArtifact().getVersion());
    }
  }
}
