/*
 * Copyright © 2015-2022 Cask Data, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.stream.JsonWriter;
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
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.deploy.Manager;
import io.cdap.cdap.app.deploy.ManagerFactory;
import io.cdap.cdap.app.store.ApplicationFilter;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.CannotBeDeletedException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.utils.BatchingConsumer;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.DefaultApplicationUpdateContext;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentRuntimeInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.app.store.state.AppStateKey;
import io.cdap.cdap.internal.app.store.state.AppStateKeyValue;
import io.cdap.cdap.internal.capability.CapabilityNotAvailableException;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.artifact.ChangeSummary;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that manage lifecycle of Applications.
 */
public class ApplicationLifecycleService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationLifecycleService.class);
  private static final Gson GSON =
      new GsonBuilder().registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
          .create();

  /**
   * Store manages non-runtime lifecycle.
   */
  private final CConfiguration cConf;
  private final Store store;
  private final Scheduler scheduler;
  private final UsageRegistry usageRegistry;
  private final PreferencesService preferencesService;
  private final MetricsSystemClient metricsSystemClient;
  private final OwnerAdmin ownerAdmin;
  private final ArtifactRepository artifactRepository;
  private final ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory;
  private final MetadataServiceClient metadataServiceClient;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final boolean appUpdateSchedules;
  private final AdminEventPublisher adminEventPublisher;
  private final Impersonator impersonator;
  private final CapabilityReader capabilityReader;
  private final int batchSize;
  private final MetricsCollectionService metricsCollectionService;
  private final FeatureFlagsProvider featureFlagsProvider;

  /**
   * Construct the ApplicationLifeCycleService with service factory and cConf coming from guice
   * injection.
   */
  @Inject
  public ApplicationLifecycleService(CConfiguration cConf,
      Store store, Scheduler scheduler, UsageRegistry usageRegistry,
      PreferencesService preferencesService, MetricsSystemClient metricsSystemClient,
      OwnerAdmin ownerAdmin, ArtifactRepository artifactRepository,
      ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms> managerFactory,
      MetadataServiceClient metadataServiceClient,
      AccessEnforcer accessEnforcer, AuthenticationContext authenticationContext,
      MessagingService messagingService, Impersonator impersonator,
      CapabilityReader capabilityReader,
      MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.appUpdateSchedules = cConf.getBoolean(Constants.AppFabric.APP_UPDATE_SCHEDULES,
        Constants.AppFabric.DEFAULT_APP_UPDATE_SCHEDULES);
    this.batchSize = cConf.getInt(AppFabric.STREAMING_BATCH_SIZE);
    this.store = store;
    this.scheduler = scheduler;
    this.usageRegistry = usageRegistry;
    this.preferencesService = preferencesService;
    this.metricsSystemClient = metricsSystemClient;
    this.artifactRepository = artifactRepository;
    this.managerFactory = managerFactory;
    this.metadataServiceClient = metadataServiceClient;
    this.ownerAdmin = ownerAdmin;
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
    this.impersonator = impersonator;
    this.capabilityReader = capabilityReader;
    this.adminEventPublisher = new AdminEventPublisher(cConf,
        new MultiThreadMessagingContext(messagingService));
    this.metricsCollectionService = metricsCollectionService;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
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
   * Scans all applications in the specified namespace, filtered to only include applications with
   * an artifact name in the set of specified names and an artifact version equal to the specified
   * version. If the specified set is empty, no filtering is performed on artifact name. If the
   * specified version is null, no filtering is done on artifact version.
   *
   * @param namespace the namespace to scan apps from
   * @param artifactNames the set of returned artifact names. If empty, all artifact names are
   *     returned
   * @param artifactVersion the artifact version to match. If null, all artifact versions are
   *     returned
   * @param consumer a {@link Consumer} to consume each ApplicationDetail being scanned
   */
  public void scanApplications(NamespaceId namespace, Set<String> artifactNames,
      @Nullable String artifactVersion,
      Consumer<ApplicationDetail> consumer) {

    List<ApplicationFilter> filters = getAppFilters(artifactNames, artifactVersion);
    scanApplications(namespace, filters, consumer);
  }

  /**
   * Scans all the latest applications in the specified namespace, filtered to only include application details.
   * which satisfy the filters
   *
   * @param namespace the namespace to scan apps from
   * @param filters the filters that must be satisfied  by ApplicationDetail in order to be
   *     returned
   * @param consumer a {@link Consumer} to consume each ApplicationDetail being scanned
   */
  public void scanApplications(NamespaceId namespace, List<ApplicationFilter> filters,
      Consumer<ApplicationDetail> consumer) {
    scanApplications(ScanApplicationsRequest.builder()
        .setNamespaceId(namespace)
        .addFilters(filters)
        .setLatestOnly(true)
        .build(), consumer);
  }

  /**
   * Scans all applications in the specified namespace, filtered to only include application details.
   * which satisfy the filters
   *
   * @param request application scan request. Must name namespace filled
   * @param consumer a {@link Consumer} to consume each ApplicationDetail being scanned
   * @return if limit was reached (true) or all items were scanned before reaching the limit (false)
   * @throws IllegalArgumentException if scan request does not have namespace specified
   */
  public boolean scanApplications(ScanApplicationsRequest request,
      Consumer<ApplicationDetail> consumer) {
    NamespaceId namespace = request.getNamespaceId();
    if (namespace == null) {
      throw new IllegalStateException("Application scan request without namespace");
    }
    accessEnforcer.enforceOnParent(EntityType.DATASET, namespace,
        authenticationContext.getPrincipal(), StandardPermission.LIST);

    try (
        BatchingConsumer<Entry<ApplicationId, ApplicationMeta>> batchingConsumer = new BatchingConsumer<>(
            list -> processApplications(list, consumer), batchSize
        )
    ) {

      return store.scanApplications(request, batchSize,
          (appId, appMeta) -> batchingConsumer.accept(new SimpleEntry<>(appId, appMeta)));
    }
  }

  private void processApplications(List<Map.Entry<ApplicationId, ApplicationMeta>> list,
      Consumer<ApplicationDetail> consumer) {

    Set<ApplicationId> appIds = list.stream().map(Map.Entry::getKey).collect(Collectors.toSet());

    Set<? extends EntityId> visible = accessEnforcer.isVisible(appIds,
        authenticationContext.getPrincipal());

    list.removeIf(entry -> !visible.contains(entry.getKey()));
    appIds.removeIf(id -> !visible.contains(id));

    try {
      Map<ApplicationId, String> owners = ownerAdmin.getOwnerPrincipals(appIds);

      for (Map.Entry<ApplicationId, ApplicationMeta> entry : list) {
        ApplicationDetail applicationDetail = ApplicationDetail.fromSpec(entry.getValue().getSpec(),
            owners.get(entry.getKey()),
            entry.getValue().getChange(),
            entry.getValue().getSourceControlMeta());

        try {
          capabilityReader.checkAllEnabled(entry.getValue().getSpec());
        } catch (CapabilityNotAvailableException ex) {
          LOG.debug("Application {} is ignored due to exception.",
              applicationDetail.getName(), ex);
          continue;
        }

        consumer.accept(applicationDetail);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get detail about the specified application.
   *
   * @param appId the id of the application to get
   * @return detail about the specified application
   * @throws ApplicationNotFoundException if the specified application does not exist
   */
  public ApplicationDetail getAppDetail(ApplicationId appId) throws Exception {
    // user needs to pass the visibility check to get the app detail
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.GET);
    ApplicationMeta appMeta = store.getApplicationMetadata(appId);
    if (appMeta == null || appMeta.getSpec() == null) {
      throw new ApplicationNotFoundException(appId);
    }
    String ownerPrincipal = ownerAdmin.getOwnerPrincipal(appId);
    return enforceApplicationDetailAccess(appId,
        ApplicationDetail.fromSpec(appMeta.getSpec(), ownerPrincipal,
            appMeta.getChange(),
            appMeta.getSourceControlMeta()));
  }

  public ApplicationDetail getLatestAppDetail(ApplicationReference appRef)
      throws IOException, NotFoundException {
    return getLatestAppDetail(appRef, true);
  }

  /**
   * Get details about the latest version of the specified application.
   *
   * @param appRef the application reference
   * @param getSourceControlMeta the boolean indicating if we return {@link SourceControlMeta}
   * @return detail about the latest version of the specified application
   */
  public ApplicationDetail getLatestAppDetail(ApplicationReference appRef,
      boolean getSourceControlMeta)
      throws IOException, NotFoundException {
    ApplicationMeta appMeta = store.getLatest(appRef);
    if (appMeta == null || appMeta.getSpec() == null) {
      throw new ApplicationNotFoundException(appRef);
    }

    ApplicationId appId = appRef.app(appMeta.getSpec().getAppVersion());
    String ownerPrincipal = ownerAdmin.getOwnerPrincipal(appId);
    return enforceApplicationDetailAccess(appId,
        ApplicationDetail.fromSpec(appMeta.getSpec(), ownerPrincipal,
            appMeta.getChange(),
            getSourceControlMeta
                ? appMeta.getSourceControlMeta() : null));
  }

  /**
   * Gets details for a set of given applications.
   *
   * @param appIds the set of application id to get details
   * @return a {@link Map} from the application id to the corresponding detail. There will be no
   *     entry for applications that don't exist.
   * @throws Exception if failed to get details.
   */
  public Map<ApplicationId, ApplicationDetail> getAppDetails(Collection<ApplicationId> appIds)
      throws Exception {
    Set<? extends EntityId> visibleIds = accessEnforcer.isVisible(new HashSet<>(appIds),
        authenticationContext.getPrincipal());
    Set<ApplicationId> filterIds = appIds.stream().filter(visibleIds::contains)
        .collect(Collectors.toSet());
    Map<ApplicationId, ApplicationMeta> appMetas = store.getApplications(filterIds);
    Map<ApplicationId, String> principals = ownerAdmin.getOwnerPrincipals(filterIds);

    return appMetas.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        entry -> ApplicationDetail.fromSpec(entry.getValue().getSpec(),
            principals.get(entry.getKey()),
            entry.getValue().getChange(),
            entry.getValue().getSourceControlMeta())
    ));
  }

  /**
   * Creates a ZIP archive that contains the {@link ApplicationDetail} for all applications. The
   * archive created will contain a directory entry for each of the namespace. Inside each namespace
   * directory, it contains the application detail json, the application name as the file name, with
   * {@code ".json"} as the file extension.
   * <p/>
   * This method requires instance admin permission.
   *
   * @param zipOut the {@link ZipOutputStream} for writing out the application details
   */
  public void createAppDetailsArchive(ZipOutputStream zipOut) {
    accessEnforcer.enforce(new InstanceId(cConf.get(Constants.INSTANCE_NAME)),
        authenticationContext.getPrincipal(), StandardPermission.GET);

    Set<NamespaceId> namespaces = new HashSet<>();

    JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(zipOut, StandardCharsets.UTF_8));
    store.scanApplications(batchSize, (appId, appMeta) -> {
      // Skip the SYSTEM namespace apps
      if (NamespaceId.SYSTEM.equals(appId.getParent())) {
        return;
      }
      try {
        ApplicationDetail applicationDetail = enforceApplicationDetailAccess(
            appId, ApplicationDetail.fromSpec(appMeta.getSpec(), null,
                appMeta.getChange(), appMeta.getSourceControlMeta()));
        // Add a directory for the namespace
        if (namespaces.add(appId.getParent())) {
          ZipEntry entry = new ZipEntry(appId.getNamespace() + "/");
          zipOut.putNextEntry(entry);
          zipOut.closeEntry();
        }
        ZipEntry entry = new ZipEntry(
            appId.getNamespace() + "/" + appId.getApplication() + ".json");
        zipOut.putNextEntry(entry);
        GSON.toJson(applicationDetail, ApplicationDetail.class, jsonWriter);
        jsonWriter.flush();
        zipOut.closeEntry();

      } catch (IOException | AccessException e) {
        throw new RuntimeException("Failed to add zip entry for application " + appId, e);
      }
    });
  }

  /**
   * Returns all the application versions for the given application name.
   *
   * @param appRef the application reference
   * @return a collection of version strings
   */
  public Collection<String> getAppVersions(ApplicationReference appRef) {
    Collection<ApplicationId> ids = store.getAllAppVersionsAppIds(appRef);
    // Hide -SNAPSHOT when more than one version of the app exists
    if (ids.size() > 1) {
      return ids.stream()
          .map(ApplicationId::getVersion)
          .filter(version -> !ApplicationId.DEFAULT_VERSION.equals(version))
          .collect(Collectors.toList());
    }
    return ids.stream()
        .map(ApplicationId::getVersion)
        .collect(Collectors.toList());
  }

  /**
   * Update an existing application. An application's configuration and artifact version can be
   * updated.
   *
   * @param appId the id of the application to update
   * @param appRequest the request to update the application, including new config and artifact
   * @param programTerminator a program terminator that will stop programs that are removed when
   *     updating an app. For example, if an update removes a flow, the terminator defines how to
   *     stop that flow.
   * @return information about the deployed application
   * @throws ApplicationNotFoundException if the specified application does not exist
   * @throws ArtifactNotFoundException if the requested artifact does not exist
   * @throws InvalidArtifactException if the specified artifact is invalid. For example, if the
   *     artifact name changed, if the version is an invalid version, or the artifact contains no
   *     app classes
   * @throws Exception if there was an exception during the deployment pipeline. This exception
   *     will often wrap the actual exception
   */
  public ApplicationWithPrograms updateApp(ApplicationId appId, AppRequest appRequest,
      ProgramTerminator programTerminator) throws Exception {
    // Check if the current user has admin privileges on it before updating.
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);

    ApplicationMeta currentApp = store.getLatest(appId.getAppReference());
    ApplicationSpecification currentSpec = Optional.ofNullable(currentApp)
        .map(ApplicationMeta::getSpec)
        .orElse(null);
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
        throw new InvalidArtifactException("Only artifact version updates are allowed. "
            + "Cannot change from a non-system artifact to a system artifact or vice versa.");
      }

      // check requested artifact version is valid
      ArtifactVersion requestedVersion = new ArtifactVersion(requestedArtifact.getVersion());
      if (requestedVersion.getVersion() == null) {
        throw new InvalidArtifactException(String.format(
            "Requested artifact version '%s' is invalid", requestedArtifact.getVersion()));
      }
      newArtifactId = new ArtifactId(currentArtifact.getName(), requestedVersion,
          currentArtifact.getScope());
    }

    // ownerAdmin.getImpersonationPrincipal will give the owner which will be impersonated for the application
    // irrespective of the version
    SecurityUtil.verifyOwnerPrincipal(appId, appRequest.getOwnerPrincipal(), ownerAdmin);

    Object requestedConfigObj = appRequest.getConfig();
    // if config is null, use the previous config. Shouldn't use a static GSON since the request Config object can
    // be a user class, otherwise there will be ClassLoader leakage.
    String requestedConfigStr = requestedConfigObj == null
        ? currentSpec.getConfiguration() : new Gson().toJson(requestedConfigObj);

    Id.Artifact artifactId = Id.Artifact.fromEntityId(
        Artifacts.toProtoArtifactId(appId.getParent(), newArtifactId));
    String versionId = currentSpec.getAppVersion();
    // If LCM flow is enabled - we generate specific versions of the app.
    if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
      versionId = RunIds.generate().getId();
    }
    return deployApp(appId.getParent(), appId.getApplication(), versionId, artifactId,
        requestedConfigStr,
        appRequest.getChange(), programTerminator, ownerAdmin.getOwner(appId),
        appRequest.canUpdateSchedules());
  }

  /**
   * Finds latest application artifact for given application and current artifact for upgrading
   * application. If no artifact found then returns current artifact as the candidate.
   *
   * @param appId application Id to find latest app artifact for.
   * @param currentArtifactId current artifact used by application.
   * @param allowedArtifactScopes artifact scopes to search in for finding candidate artifacts.
   * @param allowSnapshot whether to consider snapshot version of artifacts or not for upgrade.
   * @return {@link ArtifactSummary} for the artifact to be used for upgrade purpose.
   * @throws NotFoundException if there is no artifact available for given artifact.
   * @throws Exception if there was an exception during finding candidate artifact.
   */
  private ArtifactSummary getLatestAppArtifactForUpgrade(ApplicationId appId,
      ArtifactId currentArtifactId,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot)
      throws Exception {

    List<ArtifactSummary> availableArtifacts = new ArrayList<>();
    // At the least, current artifact should be in the set of available artifacts.
    availableArtifacts.add(ArtifactSummary.from(currentArtifactId));

    // Find candidate artifacts from all scopes we need to consider.
    for (ArtifactScope scope : allowedArtifactScopes) {
      NamespaceId artifactNamespaceToConsider =
          ArtifactScope.SYSTEM.equals(scope) ? NamespaceId.SYSTEM : appId.getParent();
      List<ArtifactSummary> artifacts;
      try {
        artifacts = artifactRepository.getArtifactSummaries(artifactNamespaceToConsider,
            currentArtifactId.getName(),
            Integer.MAX_VALUE, ArtifactSortOrder.ASC);
      } catch (ArtifactNotFoundException e) {
        // This can happen if we are looking for candidate artifact in multiple namespace.
        continue;
      }
      for (ArtifactSummary artifactSummary : artifacts) {
        ArtifactVersion artifactVersion = new ArtifactVersion(artifactSummary.getVersion());
        // Consider if it is a non-snapshot version artifact or it is a snapshot version than allowSnapshot is true.
        if ((artifactVersion.isSnapshot() && allowSnapshot) || !artifactVersion.isSnapshot()) {
          availableArtifacts.add(artifactSummary);
        }
      }
    }

    // Find the artifact with latest version.
    Optional<ArtifactSummary> newArtifactCandidate = availableArtifacts.stream().max(
        Comparator.comparing(artifactSummary -> new ArtifactVersion(artifactSummary.getVersion())));

    io.cdap.cdap.proto.id.ArtifactId currentArtifact =
        new io.cdap.cdap.proto.id.ArtifactId(appId.getNamespace(), currentArtifactId.getName(),
            currentArtifactId.getVersion().getVersion());
    return newArtifactCandidate.orElseThrow(() -> new ArtifactNotFoundException(currentArtifact));
  }

  /**
   * Upgrades an existing application by upgrading application artifact versions and plugin artifact
   * versions.
   *
   * @param appId the id of the application to upgrade.
   * @param allowedArtifactScopes artifact scopes allowed while looking for latest artifacts for
   *     upgrade.
   * @param allowSnapshot whether to consider snapshot version of artifacts or not for upgrade.
   * @return the {@link ApplicationId} of the application version being updated.
   * @throws IllegalStateException if something unexpected happened during upgrade.
   * @throws IOException if there was an IO error during initializing application class from
   *     artifact.
   * @throws JsonIOException if there was an error in serializing or deserializing app config.
   * @throws UnsupportedOperationException if application does not support upgrade operation.
   * @throws InvalidArtifactException if candidate application artifact is invalid for upgrade
   *     purpose.
   * @throws NotFoundException if any object related to upgrade is not found like
   *     application/artifact.
   * @throws Exception if there was an exception during the upgrade of application. This
   *     exception will often wrap the actual exception
   */
  @Deprecated
  public ApplicationId upgradeApplication(ApplicationId appId,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot) throws Exception {

    ApplicationMeta appMeta = store.getApplicationMetadata(appId);
    ApplicationSpecification currentSpec = Optional.ofNullable(appMeta)
        .map(ApplicationMeta::getSpec).orElse(null);

    if (currentSpec == null) {
      LOG.debug("Application {} not found for upgrade.", appId);
      throw new ApplicationNotFoundException(appId);
    }

    return updateApplicationByArtifact(appId, currentSpec, allowedArtifactScopes, allowSnapshot);
  }

  /**
   * Upgrades the latest version of an existing application by upgrading application artifact
   * versions and plugin artifact versions.
   *
   * @param appRef the application reference of the application to upgrade.
   * @param allowedArtifactScopes artifact scopes allowed while looking for latest artifacts for
   *     upgrade.
   * @param allowSnapshot whether to consider snapshot version of artifacts or not for upgrade.
   * @return the {@link ApplicationId} of the application version being updated.
   * @throws IllegalStateException if something unexpected happened during upgrade.
   * @throws IOException if there was an IO error during initializing application class from
   *     artifact.
   * @throws JsonIOException if there was an error in serializing or deserializing app config.
   * @throws UnsupportedOperationException if application does not support upgrade operation.
   * @throws InvalidArtifactException if candidate application artifact is invalid for upgrade
   *     purpose.
   * @throws NotFoundException if any object related to upgrade is not found like
   *     application/artifact.
   * @throws Exception if there was an exception during the upgrade of application. This
   *     exception will often wrap the actual exception
   */
  public ApplicationId upgradeLatestApplication(ApplicationReference appRef,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot) throws Exception {

    ApplicationMeta appMeta = store.getLatest(appRef);
    ApplicationSpecification currentSpec = Optional.ofNullable(appMeta)
        .map(ApplicationMeta::getSpec).orElse(null);

    if (currentSpec == null) {
      LOG.debug("Application {} not found for upgrade.", appRef);
      throw new ApplicationNotFoundException(appRef);
    }

    ApplicationId currentAppId = appRef.app(currentSpec.getAppVersion());

    return updateApplicationByArtifact(currentAppId, currentSpec, allowedArtifactScopes,
        allowSnapshot);
  }

  private ApplicationId updateApplicationByArtifact(ApplicationId appId,
      ApplicationSpecification appSpec,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot) throws Exception {
    // Check if the current user has admin privileges on it before updating.
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.UPDATE);

    ArtifactId currentArtifact = appSpec.getArtifactId();
    ArtifactSummary candidateArtifact = getLatestAppArtifactForUpgrade(appId, currentArtifact,
        allowedArtifactScopes,
        allowSnapshot);
    ArtifactVersion candidateArtifactVersion = new ArtifactVersion(candidateArtifact.getVersion());

    // Current artifact should not have higher version than candidate artifact.
    if (currentArtifact.getVersion().compareTo(candidateArtifactVersion) > 0) {
      String error = String.format(
          "The current artifact has a version higher %s than any existing artifact.",
          currentArtifact.getVersion());
      throw new InvalidArtifactException(error);
    }

    ArtifactId newArtifactId =
        new ArtifactId(candidateArtifact.getName(), candidateArtifactVersion,
            candidateArtifact.getScope());

    Id.Artifact newArtifact = Id.Artifact.fromEntityId(
        Artifacts.toProtoArtifactId(appId.getParent(),
            newArtifactId));
    ArtifactDetail newArtifactDetail = artifactRepository.getArtifact(newArtifact);

    return updateApplicationInternal(appId, programId -> {
        }, newArtifactDetail,
        Collections.singletonList(ApplicationConfigUpdateAction.UPGRADE_ARTIFACT),
        allowedArtifactScopes, allowSnapshot, ownerAdmin.getOwner(appId), appSpec);
  }

  /**
   * Updates an application config by applying given update actions. The app should know how to
   * apply these actions to its config.
   */
  private ApplicationId updateApplicationInternal(ApplicationId appId,
      ProgramTerminator programTerminator,
      ArtifactDetail artifactDetail,
      List<ApplicationConfigUpdateAction> updateActions,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot,
      @Nullable KerberosPrincipalId ownerPrincipal,
      ApplicationSpecification appSpec) throws Exception {
    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(),
        null);
    if (appClass == null) {
      // This should never happen.
      throw new IllegalStateException(
          String.format("No application class found in artifact '%s' in namespace '%s'.",
              artifactDetail.getDescriptor().getArtifactId(), appId.getParent()));
    }
    io.cdap.cdap.proto.id.ArtifactId artifactId =
        Artifacts.toProtoArtifactId(appId.getParent(),
            artifactDetail.getDescriptor().getArtifactId());
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId,
        this.impersonator);

    String updatedAppConfig;
    DefaultApplicationUpdateContext updateContext =
        new DefaultApplicationUpdateContext(appId.getParent(), appId,
            artifactDetail.getDescriptor().getArtifactId(),
            artifactRepository, appSpec.getConfiguration(), updateActions,
            allowedArtifactScopes, allowSnapshot, appSpec);

    try (CloseableClassLoader artifactClassLoader =
        artifactRepository.createArtifactClassLoader(artifactDetail.getDescriptor(),
            classLoaderImpersonator)) {
      Object appMain = artifactClassLoader.loadClass(appClass.getClassName()).newInstance();
      // Run config update logic for the application to generate updated config.
      if (!(appMain instanceof Application)) {
        throw new IllegalStateException(
            String.format("Application main class is of invalid type: %s",
                appMain.getClass().getName()));
      }
      Application<?> app = (Application<?>) appMain;
      Type configType = Artifacts.getConfigType(app.getClass());
      if (!app.isUpdateSupported()) {
        String errorMessage = String.format("Application %s does not support update.", appId);
        throw new UnsupportedOperationException(errorMessage);
      }
      ApplicationUpdateResult<?> updateResult = app.updateConfig(updateContext);
      updatedAppConfig = GSON.toJson(updateResult.getNewConfig(), configType);
    }
    Principal requestingUser = authenticationContext.getPrincipal();

    String versionId = appId.getVersion();
    // If LCM flow is enabled - we generate specific versions of the app.
    if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
      versionId = RunIds.generate().getId();
    }

    // Deploy application with with potentially new app config and new artifact.
    AppDeploymentInfo deploymentInfo = AppDeploymentInfo.builder()
        .setArtifactId(artifactId)
        .setArtifactLocation(artifactDetail.getDescriptor().getLocation())
        .setApplicationClass(appClass)
        .setNamespaceId(appId.getNamespaceId())
        .setAppName(appId.getApplication())
        .setAppVersion(versionId)
        .setConfigString(updatedAppConfig)
        .setOwnerPrincipal(ownerPrincipal)
        .setUpdateSchedules(false)
        .setChangeDetail(new ChangeDetail(null, appId.getVersion(), requestingUser == null ? null :
            requestingUser.getName(), System.currentTimeMillis()))
        .setDeployedApplicationSpec(appSpec)
        .setIsUpgrade(true)
        .build();

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(
        programTerminator);
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

    return applicationWithPrograms.getApplicationId();
  }

  /**
   * Deploy an application by first adding the application jar to the artifact repository, then
   * creating an application using that newly added artifact.
   *
   * @param namespace the namespace to deploy the application and artifact in
   * @param appName the name of the app. If null, the name will be set based on the application
   *     spec
   * @param artifactId the id of the artifact to add and create the application from
   * @param jarFile the application jar to add as an artifact and create the application from
   * @param configStr the configuration to send to the application when generating the
   *     application specification
   * @param programTerminator a program terminator that will stop programs that are removed when
   *     updating an app. For example, if an update removes a flow, the terminator defines how to
   *     stop that flow.
   * @return information about the deployed application
   * @throws InvalidArtifactException the the artifact is invalid. For example, if it does not
   *     contain any app classes
   * @throws ArtifactAlreadyExistsException if the specified artifact already exists
   * @throws IOException if there was an IO error writing the artifact
   */
  public ApplicationWithPrograms deployAppAndArtifact(NamespaceId namespace,
      @Nullable String appName,
      Id.Artifact artifactId, File jarFile,
      @Nullable String configStr,
      @Nullable KerberosPrincipalId ownerPrincipal,
      ProgramTerminator programTerminator,
      boolean updateSchedules) throws Exception {

    ArtifactDetail artifactDetail = artifactRepository.addArtifact(artifactId, jarFile);
    try {
      String appVersion = ApplicationId.DEFAULT_VERSION;
      // If LCM flow is enabled - we generate specific versions of the app.
      if (Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)) {
        appVersion = RunIds.generate().getId();
      }
      return deployApp(namespace, appName, appVersion, configStr, null, null, programTerminator,
          artifactDetail,
          ownerPrincipal, updateSchedules, false, Collections.emptyMap());
    } catch (Exception e) {
      // if we added the artifact, but failed to deploy the application, delete the artifact to bring us back
      // to the state we were in before this call.
      try {
        artifactRepository.deleteArtifact(artifactId);
      } catch (Exception e2) {
        // if the delete fails, nothing we can do, just log it and continue on
        LOG.warn(
            "Failed to delete artifact {} after deployment of artifact and application failed.",
            artifactId, e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed,
   * the Application class is instantiated and configure() is called in order to generate an {@link
   * ApplicationSpecification}. Programs, datasets, and streams are created based on the
   * specification before the spec is persisted in the {@link Store}. This method can create a new
   * application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application
   *     spec
   * @param artifactId the id of the artifact to create the application from
   * @param programTerminator a program terminator that will stop programs that are removed when
   *     updating an app. For example, if an update removes a flow, the terminator defines how to
   *     stop that flow.
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws Exception if there was an exception during the deployment pipeline. This exception
   *     will often wrap the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace,
      @Nullable String appName,
      Id.Artifact artifactId,
      @Nullable String configStr,
      ProgramTerminator programTerminator) throws Exception {
    String appVersion = Feature.LIFECYCLE_MANAGEMENT_EDIT.isEnabled(featureFlagsProvider)
        ? RunIds.generate().getId()
        : ApplicationId.DEFAULT_VERSION;
    return deployApp(namespace, appName, appVersion, artifactId, configStr, null, programTerminator,
        null, true);
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed,
   * the Application class is instantiated and configure() is called in order to generate an {@link
   * ApplicationSpecification}. Programs, datasets, and streams are created based on the
   * specification before the spec is persisted in the {@link Store}. This method can create a new
   * application as well as update an existing one.
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application
   *     spec
   * @param artifactId the id of the artifact to create the application from
   * @param configStr the configuration to send to the application when generating the
   *     application specification
   * @param changeSummary the change summary entered by the user
   * @param programTerminator a program terminator that will stop programs that are removed when
   *     updating an app. For example, if an update removes a flow, the terminator defines how to
   *     stop that flow.
   * @param ownerPrincipal the kerberos principal of the application owner
   * @param updateSchedules specifies if schedules of the workflow have to be updated, if null
   *     value specified by the property "app.deploy.update.schedules" will be used.
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws Exception if there was an exception during the deployment pipeline. This exception
   *     will often wrap the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace, @Nullable String appName,
      @Nullable String appVersion,
      Id.Artifact artifactId,
      @Nullable String configStr,
      @Nullable ChangeSummary changeSummary,
      ProgramTerminator programTerminator,
      @Nullable KerberosPrincipalId ownerPrincipal,
      @Nullable Boolean updateSchedules) throws Exception {
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(artifactId);
    return deployApp(namespace, appName, appVersion, configStr, changeSummary, null,
        programTerminator,
        artifactDetail, ownerPrincipal,
        updateSchedules == null ? appUpdateSchedules : updateSchedules,
        false, Collections.emptyMap());
  }

  /**
   * Deploy an application using the specified artifact and configuration. When an app is deployed,
   * the Application class is instantiated and configure() is called in order to generate an {@link
   * ApplicationSpecification}. Programs, datasets, and streams are created based on the
   * specification before the spec is persisted in the {@link Store}. This method can create a new
   * application as well as update an existing one (when LifecycleManagement feature is disabled).
   *
   * @param namespace the namespace to deploy the app to
   * @param appName the name of the app. If null, the name will be set based on the application
   *     spec
   * @param summary the artifact summary of the app
   * @param configStr the configuration to send to the application when generating the
   *     application specification
   * @param changeSummary the change summary entered by the user - includes the description and
   *     parent-version
   * @param sourceControlMeta the source control metadata of an application that is pulled from
   *     linked git repository
   * @param programTerminator a program terminator that will stop programs that are removed when
   *     updating an app. For example, if an update removes a flow, the terminator defines how to
   *     stop that flow.
   * @param ownerPrincipal the kerberos principal of the application owner
   * @param updateSchedules specifies if schedules of the workflow have to be updated, if null
   *     value specified by the property "app.deploy.update.schedules" will be used.
   * @param isPreview whether the app deployment is for preview
   * @param userProps the user properties for the app deployment, this is basically used for
   *     preview deployment
   * @return information about the deployed application
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws Exception if there was an exception during the deployment pipeline. This exception
   *     will often wrap the actual exception
   */
  public ApplicationWithPrograms deployApp(NamespaceId namespace, @Nullable String appName,
      @Nullable String appVersion,
      ArtifactSummary summary,
      @Nullable String configStr,
      @Nullable ChangeSummary changeSummary,
      @Nullable SourceControlMeta sourceControlMeta,
      ProgramTerminator programTerminator,
      @Nullable KerberosPrincipalId ownerPrincipal,
      @Nullable Boolean updateSchedules, boolean isPreview,
      Map<String, String> userProps)
      throws Exception {
    // TODO CDAP-19828 - remove appVersion parameter from method signature
    NamespaceId artifactNamespace =
        ArtifactScope.SYSTEM.equals(summary.getScope()) ? NamespaceId.SYSTEM : namespace;
    ArtifactRange range = new ArtifactRange(artifactNamespace.getNamespace(), summary.getName(),
        ArtifactVersionRange.parse(summary.getVersion()));
    // this method will not throw ArtifactNotFoundException, if no artifacts in the range, we are expecting an empty
    // collection returned.
    List<ArtifactDetail> artifactDetail = artifactRepository.getArtifactDetails(range, 1,
        ArtifactSortOrder.DESC);
    if (artifactDetail.isEmpty()) {
      throw new ArtifactNotFoundException(range.getNamespace(), range.getName(), range.getVersionString());
    }
    return deployApp(namespace, appName, appVersion, configStr, changeSummary, sourceControlMeta,
        programTerminator,
        artifactDetail.get(0), ownerPrincipal, updateSchedules == null
            ? appUpdateSchedules : updateSchedules, isPreview, userProps);
  }

  /**
   * Deploy an application using the specified {@link AppRequest}. When an app is deployed, the
   * Application class is instantiated and configure() is called in order to generate an {@link
   * ApplicationSpecification}. Programs, datasets, and streams are created based on the
   * specification before the spec is persisted in the {@link Store}. This method can create a new
   * application as well as update an existing one (when LifecycleManagement feature is disabled).
   *
   * @param appId The {@link ApplicationId} to deploy with
   * @param appRequest The {@link AppRequest}. It could be from user request or source control
   *     management.
   * @param sourceControlMeta the {@link SourceControlMeta}  of an application that is pulled
   *     from linked git repository
   * @param programTerminator a program terminator that will stop programs that are removed when
   *     updating an app. For example, if an update removes a flow, the terminator defines how to
   *     stop that flow.
   * @return {@link ApplicationWithPrograms}
   * @throws InvalidArtifactException if the artifact does not contain any application classes
   * @throws IOException if there was an IO error reading artifact detail from the meta store
   * @throws ArtifactNotFoundException if the specified artifact does not exist
   * @throws Exception if there was an exception during the deployment pipeline. This exception
   *     will often wrap the actual exception
   */
  public ApplicationWithPrograms deployApp(ApplicationId appId,
      AppRequest<?> appRequest,
      @Nullable SourceControlMeta sourceControlMeta,
      ProgramTerminator programTerminator) throws Exception {
    ArtifactSummary artifactSummary = appRequest.getArtifact();

    KerberosPrincipalId ownerPrincipalId =
        appRequest.getOwnerPrincipal() == null ? null
            : new KerberosPrincipalId(appRequest.getOwnerPrincipal());

    // if we don't null check, it gets serialized to "null". The instanceof check is also needed otherwise it causes
    // unnecessary json serialization and invalid json format error.
    Object config = appRequest.getConfig();
    String configString = config == null ? null :
        config instanceof String ? (String) config : GSON.toJson(config);

    ChangeSummary changeSummary = appRequest.getChange();

    return deployApp(appId.getParent(), appId.getApplication(), appId.getVersion(), artifactSummary,
        configString,
        changeSummary, sourceControlMeta, programTerminator, ownerPrincipalId,
        appRequest.canUpdateSchedules(), false, Collections.emptyMap());
  }

  private ApplicationWithPrograms deployApp(NamespaceId namespaceId, @Nullable String appName,
      @Nullable String appVersion,
      @Nullable String configStr,
      @Nullable ChangeSummary changeSummary,
      @Nullable SourceControlMeta sourceControlMeta,
      ProgramTerminator programTerminator,
      ArtifactDetail artifactDetail,
      @Nullable KerberosPrincipalId ownerPrincipal,
      boolean updateSchedules, boolean isPreview,
      Map<String, String> userProps) throws Exception {
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
      accessEnforcer.enforce(effectiveOwner, requestingUser, AccessPermission.SET_OWNER);
    }

    ApplicationClass appClass = Iterables.getFirst(artifactDetail.getMeta().getClasses().getApps(),
        null);
    if (appClass == null) {
      throw new InvalidArtifactException(
          String.format("No application class found in artifact '%s' in namespace '%s'.",
              artifactDetail.getDescriptor().getArtifactId(), namespaceId));
    }
    if (!NamespaceId.SYSTEM.equals(namespaceId)) {
      capabilityReader.checkAllEnabled(appClass.getRequirements().getCapabilities());
    }

    // TODO @sansans : Fetch owner info JIRA: CDAP-19491
    ChangeDetail change = new ChangeDetail(
        changeSummary == null ? null : changeSummary.getDescription(),
        changeSummary == null ? null : changeSummary.getParentVersion(),
        requestingUser == null ? null : requestingUser.getName(),
        System.currentTimeMillis());
    // deploy application with newly added artifact
    AppDeploymentInfo deploymentInfo = AppDeploymentInfo.builder()
        .setArtifactId(Artifacts.toProtoArtifactId(namespaceId,
            artifactDetail.getDescriptor().getArtifactId()))
        .setArtifactLocation(artifactDetail.getDescriptor().getLocation())
        .setApplicationClass(appClass)
        .setNamespaceId(namespaceId)
        .setAppName(appName)
        .setAppVersion(appVersion)
        .setConfigString(configStr)
        .setOwnerPrincipal(ownerPrincipal)
        .setUpdateSchedules(updateSchedules)
        .setRuntimeInfo(
            isPreview ? new AppDeploymentRuntimeInfo(null, userProps, Collections.emptyMap())
                : null)
        .setChangeDetail(change)
        .setSourceControlMeta(sourceControlMeta)
        .build();

    Manager<AppDeploymentInfo, ApplicationWithPrograms> manager = managerFactory.create(
        programTerminator);
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

  /**
   * Remove all the applications inside the given {@link Id.Namespace}.
   *
   * @param namespaceId the {@link NamespaceId} under which all application should be deleted
   * @throws CannotBeDeletedException if there are active programs running
   */
  public void removeAll(NamespaceId namespaceId) throws Exception {
    // Scan active running programs, throw early if found any
    Map<ProgramRunId, RunRecordDetail> runningPrograms = store.getActiveRuns(namespaceId);
    if (!runningPrograms.isEmpty()) {
      Set<String> activePrograms = new HashSet<>();
      for (Map.Entry<ProgramRunId, RunRecordDetail> runningProgram : runningPrograms.entrySet()) {
        activePrograms.add(
            runningProgram.getKey().getApplication() + ": " + runningProgram.getKey().getProgram());
      }

      String appAllRunningPrograms = Joiner.on(',').join(activePrograms);
      throw new CannotBeDeletedException(namespaceId,
          "The following programs are still running: " + appAllRunningPrograms);
    }

    // All Apps are STOPPED, delete them in a streaming fashion
    store.scanApplications(
        // Scan and delete all app versions across the namespace
        ScanApplicationsRequest.builder().setNamespaceId(namespaceId).build(),
        batchSize,
        (appId, appMeta) -> {
          accessEnforcer.enforce(appId, authenticationContext.getPrincipal(),
              StandardPermission.DELETE);
          try {
            deleteApp(appId, appMeta.getSpec());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Delete an application specified by appRef.
   *
   * @param appRef the {@link ApplicationReference} of the application to be removed
   * @throws ApplicationNotFoundException if the application to remove does not exist
   * @throws CannotBeDeletedException if there are active programs running
   */
  public void removeApplication(ApplicationReference appRef)
    throws ApplicationNotFoundException, CannotBeDeletedException, IOException {
    // enforce DELETE privileges on the app
    accessEnforcer.enforce(appRef.app(ApplicationId.DEFAULT_VERSION),
        authenticationContext.getPrincipal(), StandardPermission.DELETE);
    // The latest app is retrieved here and passed to deleteApp -
    // that deletes the schedules, triggers and other metadata info.

    ApplicationMeta appMeta = store.getLatest(appRef);
    if (appMeta == null || appMeta.getSpec() == null) {
      throw new ApplicationNotFoundException(appRef);
    }

    // since already checked DELETE privileges above, no need to check again for appDetail
    // ensure no running programs across all versions of the application
    ensureNoRunningPrograms(appRef);
    ApplicationId appId = new ApplicationId(appRef.getNamespace(), appRef.getApplication(),
                                            appMeta.getSpec().getAppVersion());
    // TODO :refactor to take application reference - CDAP-20425
    deleteApp(appId, appMeta.getSpec());
  }

  /**
   * Delete a specific application version specified by appId.
   *
   * @param appId the {@link ApplicationId} of the application to be removed
   * @throws NotFoundException if application is not found
   * @throws CannotBeDeletedException the application cannot be deleted because of running
   *     programs
   */
  public void removeApplicationVersion(ApplicationId appId)
      throws NotFoundException, CannotBeDeletedException {
    // enforce DELETE privileges on the app
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.DELETE);
    ensureNoRunningPrograms(appId);
    ApplicationSpecification spec = store.getApplication(appId);
    if (spec == null) {
      throw new NotFoundException(Id.Application.fromEntityId(appId));
    }
    deleteAppVersion(appId, spec);
  }

  /**
   * Find if the given application has running programs.
   *
   * @param appId the id of the application to find running programs for
   * @throws CannotBeDeletedException : the application cannot be deleted because of running
   *     programs
   */
  private void ensureNoRunningPrograms(ApplicationId appId) throws CannotBeDeletedException {
    //Check if all are stopped.
    assertNoRunningPrograms(store.getActiveRuns(appId), appId);
  }

  /**
   * Find if the given application has running programs.
   *
   * @param appRef the reference for a versionless application
   * @throws CannotBeDeletedException : the application cannot be deleted because of running
   *     programs
   */
  private void ensureNoRunningPrograms(ApplicationReference appRef)
      throws CannotBeDeletedException {
    //Check if all are stopped.
    assertNoRunningPrograms(store.getAllActiveRuns(appRef), appRef);
  }

  private void assertNoRunningPrograms(Map<ProgramRunId, RunRecordDetail> runningPrograms,
      EntityId id)
      throws CannotBeDeletedException {
    if (!runningPrograms.isEmpty()) {
      Set<String> activePrograms = new HashSet<>();
      for (Map.Entry<ProgramRunId, RunRecordDetail> runningProgram : runningPrograms.entrySet()) {
        activePrograms.add(runningProgram.getKey().getProgram());
      }

      String appAllRunningPrograms = Joiner.on(',').join(activePrograms);
      throw new CannotBeDeletedException(id,
          "The following programs are still running: " + appAllRunningPrograms);
    }
  }

  /**
   * Get plugin details for the latest version of the given application.
   *
   * @param appRef application reference
   * @return a list of {@link PluginInstanceDetail} for the latest version of the given application
   * @throws ApplicationNotFoundException if the given application does not exist
   * @throws IOException if failed to fetch plugin details
   */
  public List<PluginInstanceDetail> getPlugins(ApplicationReference appRef)
      throws NotFoundException, IOException {
    ApplicationMeta appMeta = store.getLatest(appRef);
    if (appMeta == null || appMeta.getSpec() == null) {
      throw new ApplicationNotFoundException(appRef);
    }
    return getPluginInstanceDetails(appMeta.getSpec());
  }

  private List<PluginInstanceDetail> getPluginInstanceDetails(ApplicationSpecification appSpec) {
    List<PluginInstanceDetail> pluginInstanceDetails = new ArrayList<>();
    for (Entry<String, Plugin> entry : appSpec.getPlugins().entrySet()) {
      pluginInstanceDetails.add(new PluginInstanceDetail(entry.getKey(), entry.getValue()));
    }
    return pluginInstanceDetails;
  }

  /**
   * Delete the metrics for an application.
   *
   * @param applicationId the application to delete metrics for.
   */
  private void deleteMetrics(ApplicationId applicationId, ApplicationSpecification spec)
      throws IOException {
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
  private void deletePreferences(ApplicationId appId, ApplicationSpecification appSpec) {
    for (io.cdap.cdap.api.app.ProgramType programType : io.cdap.cdap.api.app.ProgramType.values()) {
      for (String program : appSpec.getProgramsByType(programType)) {
        ProgramId programId = appId.program(ProgramType.valueOfApiProgramType(programType),
            program);
        preferencesService.deleteProperties(programId);
        LOG.trace("Deleted Preferences of Program : {}, {}, {}, {}", appId.getNamespace(),
            appId.getApplication(),
            programId.getType().getCategoryName(), programId.getProgram());
      }
    }
    preferencesService.deleteProperties(appId);
    LOG.trace("Deleted Preferences of Application : {}, {}", appId.getNamespace(),
        appId.getApplication());
  }

  // deletes without performs checks that no programs are running

  /**
   * Delete the specified application without performing checks that its programs are stopped.
   *
   * @param appId the {@link ApplicationId} of the application to be removed
   * @param spec the {@link ApplicationSpecification} of the application to be removed
   */
  private void deleteApp(ApplicationId appId, ApplicationSpecification spec) throws IOException {
    //Delete the schedules
    scheduler.deleteSchedules(appId);
    for (WorkflowSpecification workflowSpec : spec.getWorkflows().values()) {
      scheduler.modifySchedulesTriggeredByDeletedProgram(appId.workflow(workflowSpec.getName()));
    }

    deleteMetrics(appId, spec);
    deleteAppMetadata(appId, spec);
    store.deleteWorkflowStats(appId);
    deletePreferences(appId, spec);
    store.removeApplication(appId.getAppReference());

    try {
      // delete the owner as it has already been determined that this is the only version of the app
      ownerAdmin.delete(appId);
    } catch (Exception e) {
      LOG.warn(
          "Failed to delete app owner principal for application {} if one existed while deleting the "

              + "application.", appId);
    }

    try {
      usageRegistry.unregister(appId);
    } catch (Exception e) {
      LOG.warn("Failed to unregister usage of app: {}", appId, e);
    }

    // make sure the program profile metadata is removed
    adminEventPublisher.publishAppDeletion(appId, spec);
  }

  /**
   * Delete the specified application version without performing checks that its programs are
   * stopped.
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
   * Enforces necessary access on {@link ApplicationDetail}.
   */
  private ApplicationDetail enforceApplicationDetailAccess(ApplicationId appId,
      ApplicationDetail applicationDetail) throws AccessException {
    Principal principal = authenticationContext.getPrincipal();
    accessEnforcer.enforce(appId, authenticationContext.getPrincipal(), StandardPermission.GET);
    accessEnforcer.enforceOnParent(EntityType.DATASET, appId.getNamespaceId(), principal,
        StandardPermission.LIST);
    return applicationDetail;
  }

  /**
   * Creates list of scan filters for artifact names / version.
   *
   * @param artifactNames allow set of artifact names. All artifacts are allowed if empty
   * @param artifactVersion allowed artifact version. Any version is allowed if null
   * @return list of 0,1 or 2 filters
   */
  public List<ApplicationFilter> getAppFilters(Set<String> artifactNames,
      @Nullable String artifactVersion) {
    ImmutableList.Builder<ApplicationFilter> builder = ImmutableList.builder();
    if (!artifactNames.isEmpty()) {
      builder.add(new ApplicationFilter.ArtifactNamesInFilter(artifactNames));
    }
    if (artifactVersion != null) {
      builder.add(new ApplicationFilter.ArtifactVersionFilter(artifactVersion));
    }
    return builder.build();
  }

  /**
   * Decode User Id from {@link AuthenticationContext}.
   *
   * @return the User Id
   */
  public String decodeUserId(AuthenticationContext authenticationContext) {
    String decodedUserId = "emptyUserId";
    try {
      byte[] decodedBytes = Base64.getDecoder()
          .decode(authenticationContext.getPrincipal().getName());
      decodedUserId = new String(decodedBytes);
    } catch (Exception e) {
      LOG.debug("Failed to decode userId", e);
    }
    return decodedUserId;
  }

  /**
   * Get application state.
   *
   * @param request a {@link AppStateKey} object.
   * @return state of application
   * @throws ApplicationNotFoundException if application with request.appName is not found.
   */
  public Optional<byte[]> getState(AppStateKey request) throws ApplicationNotFoundException {
    emitMetrics(request.getNamespaceId().getNamespace(), request.getAppName(),
        Constants.Metrics.AppStateStore.STATE_STORE_GET_COUNT);
    long startTime = System.nanoTime();
    Optional<byte[]> state = store.getState(request);
    emitTimeMetrics(request.getNamespaceId().getNamespace(), request.getAppName(),
        Constants.Metrics.AppStateStore.STATE_STORE_GET_LATENCY_MS, startTime);
    return state;
  }

  /**
   * Save application state.
   *
   * @param request a {@link AppStateKeyValue} object.
   * @throws ApplicationNotFoundException if application with request.appName is not found.
   */
  public void saveState(AppStateKeyValue request) throws ApplicationNotFoundException {
    emitMetrics(request.getNamespaceId().getNamespace(), request.getAppName(),
        Constants.Metrics.AppStateStore.STATE_STORE_SAVE_COUNT);
    long startTime = System.nanoTime();
    store.saveState(request);
    emitTimeMetrics(request.getNamespaceId().getNamespace(), request.getAppName(),
        Constants.Metrics.AppStateStore.STATE_STORE_SAVE_LATENCY_MS, startTime);
  }

  /**
   * Delete application state.
   *
   * @param request a {@link AppStateKey} object.
   * @throws ApplicationNotFoundException if application with request.appName is not found.
   */
  public void deleteState(AppStateKey request) throws ApplicationNotFoundException {
    store.deleteState(request);
  }

  /**
   * Delete all states related to an application.
   *
   * @param namespaceId NamespaceId of the application.
   * @param appName AppName of the application.
   * @throws ApplicationNotFoundException if application with appName is not found.
   */
  public void deleteAllStates(NamespaceId namespaceId, String appName)
      throws ApplicationNotFoundException {

    // ensure that there is UPDATE permission on the app.
    accessEnforcer.enforce(namespaceId.app(appName),
        authenticationContext.getPrincipal(), StandardPermission.UPDATE);
    store.deleteAllStates(namespaceId, appName);
  }

  private void emitMetrics(String namespace, String appName, String metricName) {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, namespace,
        Constants.Metrics.Tag.APP, appName);
    metricsCollectionService.getContext(tags).increment(metricName, 1);
  }

  private void emitTimeMetrics(String namespace, String appName, String metricName,
      long startTime) {
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, namespace,
        Constants.Metrics.Tag.APP, appName);
    long timeTaken = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    metricsCollectionService.getContext(tags).gauge(metricName, timeTaken);
  }
}
