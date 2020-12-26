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
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.deploy.ProgramTerminator;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.SystemProgramManagementService;
import io.cdap.cdap.internal.entity.EntityResult;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.proto.metadata.MetadataSearchResultRecord;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.metadata.SearchRequest;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  @Inject
  CapabilityApplier(SystemProgramManagementService systemProgramManagementService,
                    ApplicationLifecycleService applicationLifecycleService, NamespaceAdmin namespaceAdmin,
                    ProgramLifecycleService programLifecycleService, CapabilityStatusStore capabilityStatusStore,
                    DiscoveryServiceClient discoveryClient) {
    this.systemProgramManagementService = systemProgramManagementService;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.capabilityStatusStore = capabilityStatusStore;
    this.namespaceAdmin = namespaceAdmin;
    this.metadataSearchClient = new MetadataSearchClient(discoveryClient);
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
      .filter(capabilityRecord -> capabilityRecord.getCapabilityOperationRecord() != null)
      .map(CapabilityRecord::getCapabilityOperationRecord)
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
      if (capabilityConfig.equals(existingConfig)) {
        capabilityStatusStore.deleteCapabilityOperation(capability);
        continue;
      }
      capabilityStatusStore.addOrUpdateCapabilityOperation(capability, CapabilityAction.ENABLE, capabilityConfig);
      LOG.debug("Enabling capability {}", capability);
      //If already deployed, will be ignored
      deployAllSystemApps(capability, capabilityConfig.getApplications());
    }
    //start all programs
    systemProgramManagementService.setProgramsEnabled(enabledPrograms);
    //mark all as enabled
    for (CapabilityConfig capabilityConfig : enableSet) {
      String capability = capabilityConfig.getCapability();
      capabilityStatusStore
        .addOrUpdateCapability(capability, CapabilityStatus.ENABLED, capabilityConfig);
      capabilityStatusStore.deleteCapabilityOperation(capability);
      LOG.debug("Enabled capability {}", capability);
    }
  }

  private void disableCapabilities(Set<CapabilityConfig> disableSet) throws Exception {
    Map<String, CapabilityConfig> configs = capabilityStatusStore
      .getConfigs(disableSet.stream().map(CapabilityConfig::getCapability).collect(Collectors.toSet()));
    for (CapabilityConfig capabilityConfig : disableSet) {
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = configs.get(capability);
      if (capabilityConfig.equals(existingConfig)) {
        capabilityStatusStore.deleteCapabilityOperation(capability);
        continue;
      }
      capabilityStatusStore.addOrUpdateCapabilityOperation(capability, CapabilityAction.DISABLE, capabilityConfig);
      LOG.debug("Disabling capability {}", capability);
      capabilityStatusStore
        .addOrUpdateCapability(capabilityConfig.getCapability(), CapabilityStatus.DISABLED, capabilityConfig);
      //stop all the programs having capability metadata. Services will be stopped by SystemProgramManagementService
      doForAllAppsWithCapability(capability,
                                 applicationId -> doWithRetry(applicationId, programLifecycleService::stopAll));
      capabilityStatusStore.deleteCapabilityOperation(capability);
      LOG.debug("Disabled capability {}", capability);
    }
  }

  private void deleteCapabilities(Set<CapabilityConfig> deleteSet) throws Exception {
    Map<String, CapabilityConfig> configs = capabilityStatusStore
      .getConfigs(deleteSet.stream().map(CapabilityConfig::getCapability).collect(Collectors.toSet()));
    for (CapabilityConfig capabilityConfig : deleteSet) {
      String capability = capabilityConfig.getCapability();
      CapabilityConfig existingConfig = configs.get(capability);
      //already deleted
      if (existingConfig == null) {
        capabilityStatusStore.deleteCapabilityOperation(capability);
        continue;
      }
      capabilityStatusStore.addOrUpdateCapabilityOperation(capability, CapabilityAction.DELETE, capabilityConfig);
      LOG.debug("Deleting capability {}", capability);
      //disable first
      disableCapabilities(deleteSet);
      //remove all applications having capability metadata.
      doForAllAppsWithCapability(capability,
                                 applicationId -> doWithRetry(applicationId,
                                                              applicationLifecycleService::removeApplication));
      //remove deployments of system applications
      for (SystemApplication application : capabilityConfig.getApplications()) {
        ApplicationId applicationId = getApplicationId(application);
        doWithRetry(applicationId, applicationLifecycleService::removeApplication);
      }
      capabilityStatusStore.deleteCapability(capability);
      capabilityStatusStore.deleteCapabilityOperation(capability);
      LOG.debug("Deleted capability {}", capability);
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

  private void deployAllSystemApps(String capability, List<SystemApplication> applications) throws Exception {
    if (applications.isEmpty()) {
      LOG.debug("Capability {} do not have apps associated with it", capability);
      return;
    }
    for (SystemApplication application : applications) {
      doWithRetry(application, this::deployApp);
    }
  }

  private void deployApp(SystemApplication application) throws Exception {
    ApplicationId applicationId = getApplicationId(application);
    LOG.debug("Deploying app {}", applicationId);
    if (isAppDeployed(applicationId)) {
      //Already deployed.
      LOG.debug("Application {} is already deployed", applicationId);
      return;
    }
    String configString = application.getConfig() == null ? null : GSON.toJson(application.getConfig());
    applicationLifecycleService
      .deployApp(applicationId.getParent(), applicationId.getApplication(), applicationId.getVersion(),
                 application.getArtifact(), configString, NOOP_PROGRAM_TERMINATOR, null, null);
  }

  private boolean isAppDeployed(ApplicationId applicationId) throws Exception {
    try {
      applicationLifecycleService.getAppDetail(applicationId);
      return true;
    } catch (ApplicationNotFoundException exception) {
      return false;
    }
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
      throwable instanceof InvalidArtifactException ||
      throwable instanceof ArtifactNotFoundException);
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
                                              int offset, int limit) throws IOException {
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
