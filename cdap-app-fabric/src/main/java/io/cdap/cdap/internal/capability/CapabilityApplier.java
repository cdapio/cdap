/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.gateway.handlers.util.VersionHelper;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.SystemProgramManagementService;
import io.cdap.cdap.internal.capability.autoinstall.HubPackage;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.metadata.SearchRequest;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class that applies capabilities
 */
class CapabilityApplier {

  private static final Logger LOG = LoggerFactory.getLogger(CapabilityApplier.class);
  private static final Gson GSON = new Gson();
  private static final int RETRY_LIMIT = 5;
  private static final int RETRY_DELAY = 5;
  private static final String CAPABILITY = "capability:%s";
  private static final String APPLICATION = "application";
  private static final ProgramTerminator NOOP_PROGRAM_TERMINATOR = programId -> {
    // no-op
  };
  private final SystemProgramManagementService systemProgramManagementService;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private final NamespaceAdmin namespaceAdmin;
  private final CapabilityStatusStore capabilityStatusStore;
  private final MetadataSearchClient metadataSearchClient;
  private final ArtifactRepository artifactRepository;
  private final File tmpDir;
  private final int autoInstallThreads;

  @Inject
  CapabilityApplier(SystemProgramManagementService systemProgramManagementService,
                    ApplicationLifecycleService applicationLifecycleService, NamespaceAdmin namespaceAdmin,
                    ProgramLifecycleService programLifecycleService, CapabilityStatusStore capabilityStatusStore,
                    ArtifactRepository artifactRepository,
                    CConfiguration cConf, RemoteClientFactory remoteClientFactory) {
    this.systemProgramManagementService = systemProgramManagementService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.capabilityStatusStore = capabilityStatusStore;
    this.namespaceAdmin = namespaceAdmin;
    this.metadataSearchClient = new MetadataSearchClient(remoteClientFactory);
    this.artifactRepository = artifactRepository;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.autoInstallThreads = cConf.getInt(Constants.Capability.AUTO_INSTALL_THREADS);
  }

  /**
   * Applies the given capability configurations
   *
   * @param capabilityConfigs Capability configurations to apply
   */
  public void apply(Collection<? extends CapabilityConfig> capabilityConfigs) throws Exception {
    List<CapabilityConfig> newConfigs = new ArrayList<>(capabilityConfigs);
    Set<CapabilityConfig> enableSet = new HashSet<>();
    Set<CapabilityConfig> disableSet = new HashSet<>();
    Set<CapabilityConfig> deleteSet = new HashSet<>();
    Map<String, CapabilityRecord> currentCapabilityRecords = capabilityStatusStore.getCapabilityRecords();
    for (CapabilityConfig newConfig : newConfigs) {
      String capability = newConfig.getCapability();
      CapabilityRecord capabilityRecord = currentCapabilityRecords.get(capability);
      if (capabilityRecord != null && capabilityRecord.getCapabilityOperationRecord() != null) {
        LOG.debug("Capability {} config for status {} skipped because there is already an operation {} in progress.",
                  capability, newConfig.getStatus(), capabilityRecord.getCapabilityOperationRecord().getActionType());
        continue;
      }
      switch (newConfig.getStatus()) {
        case ENABLED:
          enableSet.add(newConfig);
          break;
        case DISABLED:
          disableSet.add(newConfig);
          break;
        default:
          break;
      }
      currentCapabilityRecords.remove(capability);
    }
    //add all unfinished operations to retry
    List<CapabilityOperationRecord> currentOperations = currentCapabilityRecords.values().stream()
      .map(CapabilityRecord::getCapabilityOperationRecord)
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
    for (CapabilityOperationRecord operationRecord : currentOperations) {
      switch (operationRecord.getActionType()) {
        case ENABLE:
          enableSet.add(operationRecord.getCapabilityConfig());
          break;
        case DISABLE:
          disableSet.add(operationRecord.getCapabilityConfig());
          break;
        case DELETE:
          deleteSet.add(operationRecord.getCapabilityConfig());
          break;
        default:
          break;
      }
      currentCapabilityRecords.remove(operationRecord.getCapability());
    }
    // find the ones that are not being applied or retried - these should be removed
    deleteSet.addAll(currentCapabilityRecords.values().stream()
                       .filter(capabilityRecord -> capabilityRecord.getCapabilityStatusRecord() != null)
                       .map(capabilityRecord -> capabilityRecord.getCapabilityStatusRecord().getCapabilityConfig())
                       .collect(Collectors.toSet()));
    enableCapabilities(enableSet);
    disableCapabilities(disableSet);
    deleteCapabilities(deleteSet);
  }

  private void enableCapabilities(Set<CapabilityConfig> enableSet) throws Exception {
    Map<ProgramId, Arguments> enabledPrograms = new HashMap<>();
    Map<String, CapabilityConfig> configs = capabilityStatusStore
      .getConfigs(enableSet.stream().map(CapabilityConfig::getCapability).collect(Collectors.toSet()));
    for (CapabilityConfig capabilityConfig : enableSet) {
      //collect the enabled programs
      capabilityConfig.getPrograms().forEach(systemProgram -> enabledPrograms
        .put(getProgramId(systemProgram), new BasicArguments(systemProgram.getArgs())));
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = configs.get(capability);
      //Check and log so logs are not flooded
      if (!capabilityConfig.equals(existingConfig)) {
        LOG.debug("Enabling capability {}", capability);
      }
      capabilityStatusStore.addOrUpdateCapabilityOperation(capability, CapabilityAction.ENABLE, capabilityConfig);
      //If already deployed, will be ignored
      deployAllSystemApps(capability, capabilityConfig.getApplications());
      autoInstallResources(capability, capabilityConfig.getHubs());
    }
    //start all programs
    systemProgramManagementService.setProgramsEnabled(enabledPrograms);
    //mark all as enabled
    for (CapabilityConfig capabilityConfig : enableSet) {
      String capability = capabilityConfig.getCapability();
      capabilityStatusStore
        .addOrUpdateCapability(capability, CapabilityStatus.ENABLED, capabilityConfig);
      capabilityStatusStore.deleteCapabilityOperation(capability);
    }
  }

  private void disableCapabilities(Set<CapabilityConfig> disableSet) throws Exception {
    Map<String, CapabilityConfig> configs = capabilityStatusStore
      .getConfigs(disableSet.stream().map(CapabilityConfig::getCapability).collect(Collectors.toSet()));
    for (CapabilityConfig capabilityConfig : disableSet) {
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = configs.get(capability);
      //Check and log so logs are not flooded
      if (!capabilityConfig.equals(existingConfig)) {
        LOG.debug("Disabling capability {}", capability);
      }
      capabilityStatusStore.addOrUpdateCapabilityOperation(capability, CapabilityAction.DISABLE, capabilityConfig);
      capabilityStatusStore
        .addOrUpdateCapability(capabilityConfig.getCapability(), CapabilityStatus.DISABLED, capabilityConfig);
      //stop all the programs having capability metadata. Services will be stopped by SystemProgramManagementService
      doForAllAppsWithCapability(capability,
                                 applicationId -> doWithRetry(applicationId, programLifecycleService::stopAll));
      capabilityStatusStore.deleteCapabilityOperation(capability);
    }
  }

  private void deleteCapabilities(Set<CapabilityConfig> deleteSet) throws Exception {
    Map<String, CapabilityConfig> configs = capabilityStatusStore
      .getConfigs(deleteSet.stream().map(CapabilityConfig::getCapability).collect(Collectors.toSet()));
    for (CapabilityConfig capabilityConfig : deleteSet) {
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = configs.get(capability);
      if (existingConfig != null) {
        LOG.debug("Deleting capability {}", capability);
      }
      capabilityStatusStore.addOrUpdateCapabilityOperation(capability, CapabilityAction.DELETE, capabilityConfig);
      //disable first
      disableCapabilities(deleteSet);
      //remove all applications having capability metadata.
      doForAllAppsWithCapability(capability,
                                 applicationId -> doWithRetry(applicationId,
                                                              this::removeApplication));
      //remove deployments of system applications
      for (SystemApplication application : capabilityConfig.getApplications()) {
        ApplicationId applicationId = getApplicationId(application);
        doWithRetry(applicationId, this::removeApplication);
      }
      capabilityStatusStore.deleteCapability(capability);
      capabilityStatusStore.deleteCapabilityOperation(capability);
    }
  }

  private void removeApplication(ApplicationId applicationId) throws Exception {
    try {
      applicationLifecycleService.removeApplication(applicationId);
    } catch (NotFoundException ex) {
      //ignore, could have been removed with REST api or was not deployed
      LOG.debug("Application is already removed. ", ex);
    }
  }

  private ApplicationId getApplicationId(SystemApplication application) {
    String version = application.getVersion() == null ? ApplicationId.DEFAULT_VERSION : application.getVersion();
    return new ApplicationId(application.getNamespace(), application.getName(), version);
  }

  private ProgramId getProgramId(SystemProgram program) {
    ApplicationId applicationId = new ApplicationId(program.getNamespace(), program.getApplication(),
                                                    program.getVersion());
    return new ProgramId(applicationId, ProgramType.valueOf(program.getType().toUpperCase()), program.getName());
  }

  private void deployAllSystemApps(String capability, List<SystemApplication> applications)  {
    if (applications.isEmpty()) {
      //skip logging here to prevent flooding the logs
      return;
    }
    try {
      for (SystemApplication application : applications) {
        doWithRetry(application, this::deployApp);
      }
    } catch (Exception exception) {
      LOG.error("Deploying application failed", exception);
    }
  }

  private void deployApp(SystemApplication application) throws Exception {
    ApplicationId applicationId = getApplicationId(application);
    if (!shouldDeployApp(applicationId, application)) {
      //skip logging here to prevent flooding the logs
      return;
    }
    LOG.debug("Application {} is being deployed", applicationId);
    String configString = application.getConfig() == null ? null : GSON.toJson(application.getConfig());
    applicationLifecycleService
      .deployApp(applicationId.getParent(), applicationId.getApplication(), applicationId.getVersion(),
                 application.getArtifact(), configString, NOOP_PROGRAM_TERMINATOR, null, null, false,
                 Collections.emptyMap());
  }

  @VisibleForTesting
  void autoInstallResources(String capability, @Nullable List<URL> hubs) {
    ExecutorService executor = Executors.newFixedThreadPool(
      autoInstallThreads, Threads.createDaemonThreadFactory("installer-" + capability + "-%d"));

    try {
      for (URL hub : Optional.ofNullable(hubs).orElse(Collections.emptyList())) {
        HubPackage[] hubPackages;
        try {
          URL url = new URL(hub.getProtocol(), hub.getHost(), hub.getPort(), hub.getPath() + "/packages.json");
          String packagesJson = HttpClients.doGetAsString(url);
          // Deserialize packages.json from hub
          // See https://cdap.atlassian.net/wiki/spaces/DOCS/pages/554401840/Hub+API?src=search#Get-Hub-Catalog
          hubPackages = GSON.fromJson(packagesJson, HubPackage[].class);
        } catch (Exception e) {
          LOG.warn("Failed to get packages.json from {} for capability {}. Ignoring error.", hub, capability, e);
          continue;
        }

        String currentVersion = getCurrentVersion();
        // Get the latest compatible version of each plugin from hub and install it
        List<Future<?>> futures = Arrays.stream(hubPackages)
          .filter(p -> p.versionIsInRange(currentVersion))
          .collect(Collectors.groupingBy(HubPackage::getName,
                                         Collectors.maxBy(Comparator.comparing(HubPackage::getArtifactVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get)
          .map(p -> executor.submit(() -> {
            try {
              LOG.debug("Installing plugins {} for capability {} from hub {}", p.getName(), capability, hub);
              p.installPlugin(hub, artifactRepository, tmpDir);
            } catch (Exception e) {
              LOG.warn("Failed to install plugin {} for capability {} from hub {}. Ignoring error",
                       p.getName(), capability, hub, e);
            }
          }))
          .collect(Collectors.toList());
        for (Future<?> future : futures) {
          try {
            future.get();
          } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Ignoring error during plugin install", e);
          }
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @VisibleForTesting
  String getCurrentVersion() {
    return VersionHelper.getCDAPVersion().getVersion();
  }

  // Returns true if capability applier should try to deploy this application. 2 conditions when it returns true:
  // 1. Either the application is not deployed before.
  // 2. If application is deployed before then the app artifact of the deployed application is not the latest one
  //    available.
  private boolean shouldDeployApp(ApplicationId applicationId, SystemApplication application) throws Exception {
    ApplicationDetail currAppDetail;
    try {
      currAppDetail = applicationLifecycleService.getAppDetail(applicationId);
    } catch (ApplicationNotFoundException exception) {
      return true;
    }
    // Compare if the app artifact version of currently deployed application with highest version of app artifact
    // available. If it's not same, capability applier should redeploy application.
    ArtifactSummary summary = application.getArtifact();
    NamespaceId artifactNamespace =
      ArtifactScope.SYSTEM.equals(summary.getScope()) ? NamespaceId.SYSTEM : applicationId.getParent();
    ArtifactRange range = new ArtifactRange(artifactNamespace.getNamespace(), summary.getName(),
      ArtifactVersionRange.parse(summary.getVersion()));
    // this method will not throw ArtifactNotFoundException, if no artifacts in the range, we are expecting an empty
    // collection returned.
    List<ArtifactDetail> artifactDetail = artifactRepository.getArtifactDetails(range, 1, ArtifactSortOrder.DESC);
    if (artifactDetail.isEmpty()) {
      throw new ArtifactNotFoundException(range.getNamespace(), range.getName());
    }

    ArtifactId latestArtifactId = artifactDetail.get(0).getDescriptor().getArtifactId();
    // Compare the version of app artifact for deployed application and the latest available version of that
    // same artifact. If same means no need to deploy the application again.
    return !currAppDetail.getArtifact().getVersion().equals(latestArtifactId.getVersion().getVersion());
  }

  //Find all applications for capability and call consumer for each
  private void doForAllAppsWithCapability(String capability, CheckedConsumer<ApplicationId> consumer) throws Exception {
    for (NamespaceMeta namespaceMeta : namespaceAdmin.list()) {
      int offset = 0;
      int limit = 100;
      NamespaceId namespaceId = namespaceMeta.getNamespaceId();
      EntityResult<ApplicationId> results = getApplications(namespaceId, capability, null,
                                                            offset, limit);
      while (!results.getEntities().isEmpty()) {
        //call consumer for each entity
        for (ApplicationId entity : results.getEntities()) {
          consumer.accept(entity);
        }
        offset += limit;
        results = getApplications(namespaceId, capability, results.getCursor(), offset, limit);
      }
    }
  }

  private <T> void doWithRetry(T argument, CheckedConsumer<T> consumer) throws Exception {
    Retries.callWithRetries(() -> {
      consumer.accept(argument);
      return null;
    }, RetryStrategies.limit(RETRY_LIMIT, RetryStrategies.fixDelay(RETRY_DELAY, TimeUnit.SECONDS)), this::shouldRetry);
  }

  private boolean shouldRetry(Throwable throwable) {
    return !(throwable instanceof UnauthorizedException ||
      throwable instanceof InvalidArtifactException);
  }

  /**
   * Consumer functional interface that can throw exception
   *
   * @param <T>
   */
  @FunctionalInterface
  private interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }

  @VisibleForTesting
  EntityResult<ApplicationId> getApplications(NamespaceId namespace, String capability, @Nullable String cursor,
                                              int offset, int limit) throws IOException, UnauthorizedException {
    String capabilityTag = String.format(CAPABILITY, capability);
    SearchRequest searchRequest = SearchRequest.of(capabilityTag)
      .addNamespace(namespace.getNamespace())
      .addType(APPLICATION)
      .setScope(MetadataScope.SYSTEM)
      .setCursor(cursor)
      .setOffset(offset)
      .setLimit(limit)
      .build();
    MetadataSearchResponse searchResponse = metadataSearchClient.search(searchRequest);
    Set<ApplicationId> applicationIds = searchResponse.getResults().stream()
      .map(MetadataSearchResultRecord::getMetadataEntity)
      .map(this::getApplicationId)
      .collect(Collectors.toSet());
    return new EntityResult<>(applicationIds, getCursorResponse(searchResponse),
                              searchResponse.getOffset(), searchResponse.getLimit(),
                              searchResponse.getTotal());
  }

  @Nullable
  private String getCursorResponse(MetadataSearchResponse searchResponse) {
    List<String> cursors = searchResponse.getCursors();
    if (cursors == null || cursors.isEmpty()) {
      return null;
    }
    return cursors.get(0);
  }

  private ApplicationId getApplicationId(MetadataEntity metadataEntity) {
    return new ApplicationId(metadataEntity.getValue(MetadataEntity.NAMESPACE),
                             metadataEntity.getValue(MetadataEntity.APPLICATION),
                             metadataEntity.getValue(MetadataEntity.VERSION));
  }
}
