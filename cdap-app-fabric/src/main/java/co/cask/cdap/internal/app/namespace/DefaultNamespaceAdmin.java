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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizerInstantiatorService;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Admin for managing namespaces.
 */
public final class DefaultNamespaceAdmin implements NamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);

  private final Store store;
  private final NamespaceStore nsStore;
  private final PreferencesStore preferencesStore;
  private final DashboardStore dashboardStore;
  private final DatasetFramework dsFramework;
  private final ProgramRuntimeService runtimeService;
  private final QueueAdmin queueAdmin;
  private final StreamAdmin streamAdmin;
  private final MetricStore metricStore;
  private final Scheduler scheduler;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ArtifactRepository artifactRepository;
  private final AuthorizerInstantiatorService authorizerInstantiatorService;
  private final InstanceId instanceId;
  private final Pattern namespacePattern = Pattern.compile("[a-zA-Z0-9_]+");

  @Inject
  DefaultNamespaceAdmin(Store store, NamespaceStore nsStore, PreferencesStore preferencesStore,
                        DashboardStore dashboardStore, DatasetFramework dsFramework,
                        ProgramRuntimeService runtimeService, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                        MetricStore metricStore, Scheduler scheduler,
                        ApplicationLifecycleService applicationLifecycleService,
                        ArtifactRepository artifactRepository,
                        AuthorizerInstantiatorService authorizerInstantiatorService,
                        CConfiguration cConf) {
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = store;
    this.nsStore = nsStore;
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.runtimeService = runtimeService;
    this.scheduler = scheduler;
    this.metricStore = metricStore;
    this.applicationLifecycleService = applicationLifecycleService;
    this.artifactRepository = artifactRepository;
    this.authorizerInstantiatorService = authorizerInstantiatorService;
    this.instanceId = createInstanceId(cConf);
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  @Override
  public List<NamespaceMeta> list() throws Exception {
    return nsStore.list();
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   */
  @Override
  public NamespaceMeta get(Id.Namespace namespaceId) throws Exception {
    NamespaceMeta ns = nsStore.get(namespaceId);
    if (ns == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    return ns;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    try {
      get(namespaceId);
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws NamespaceAlreadyExistsException if the specified namespace already exists
   */
  @Override
  public synchronized void create(NamespaceMeta metadata) throws Exception {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    NamespaceId namespace = new NamespaceId(metadata.getName());
    if (exists(namespace.toId())) {
      throw new NamespaceAlreadyExistsException(namespace.toId());
    }

    // Namespace can be created. Check if the user is authorized now.
    Principal principal = SecurityRequestContext.toPrincipal();
    // Skip authorization enforcement for the system user and the default namespace, so the DefaultNamespaceEnsurer
    // thread can successfully create the default namespace
    if (!(Principal.SYSTEM.equals(principal) && NamespaceId.DEFAULT.equals(namespace))) {
      authorizerInstantiatorService.get().enforce(instanceId, principal, Action.ADMIN);
    }

    try {
      dsFramework.createNamespace(namespace.toId());
    } catch (DatasetManagementException e) {
      throw new NamespaceCannotBeCreatedException(namespace.toId(), e);
    }

    nsStore.create(metadata);
    // Skip authorization grants for the system user
    if (!(Principal.SYSTEM.equals(principal) && NamespaceId.DEFAULT.equals(namespace))) {
      authorizerInstantiatorService.get().grant(namespace, principal, ImmutableSet.of(Action.ALL));
    }
  }

  /**
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NamespaceCannotBeDeletedException if the specified namespace cannot be deleted
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   */
  @Override
  public synchronized void delete(final Id.Namespace namespaceId) throws Exception {
    NamespaceId namespace = namespaceId.toEntityId();
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    if (checkProgramsRunning(namespaceId.toEntityId())) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
                                                  String.format("Some programs are currently running in namespace " +
                                                                  "'%s', please stop them before deleting namespace",
                                                                namespaceId));
    }

    // Namespace can be deleted. Revoke all privileges first
    authorizerInstantiatorService.get().enforce(namespace, SecurityRequestContext.toPrincipal(), Action.ADMIN);
    authorizerInstantiatorService.get().revoke(namespace);

    LOG.info("Deleting namespace '{}'.", namespaceId);
    try {
      // Delete Preferences associated with this namespace
      preferencesStore.deleteProperties(namespaceId.getId());
      // Delete all dashboards associated with this namespace
      dashboardStore.delete(namespaceId.getId());
      // Delete all applications
      applicationLifecycleService.removeAll(namespaceId);
      // Delete all the schedules
      scheduler.deleteAllSchedules(namespaceId);
      // Delete datasets and modules
      dsFramework.deleteAllInstances(namespaceId);
      dsFramework.deleteAllModules(namespaceId);
      // Delete queues and streams data
      queueAdmin.dropAllInNamespace(namespaceId);
      streamAdmin.dropAllInNamespace(namespaceId);
      // Delete all meta data
      store.removeAll(namespaceId);

      deleteMetrics(namespaceId.toEntityId());
      // delete all artifacts in the namespace
      artifactRepository.clear(namespaceId.toEntityId());

      // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
      // create default namespace, and hence deleting it may cause undeterministic behavior.
      // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
      // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
      if (!Id.Namespace.DEFAULT.equals(namespaceId)) {
        // Finally delete namespace from MDS
        nsStore.delete(namespaceId);
        // Delete namespace in storage providers
        dsFramework.deleteNamespace(namespaceId);
      }
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId, e);
    }
    LOG.info("All data for namespace '{}' deleted.", namespaceId);
  }

  private void deleteMetrics(NamespaceId namespaceId) throws Exception {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId.getNamespace());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, tags);
    metricStore.delete(deleteQuery);
  }

  @Override
  public synchronized void deleteDatasets(Id.Namespace namespaceId) throws Exception {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    if (checkProgramsRunning(namespaceId.toEntityId())) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
                                                  String.format("Some programs are currently running in namespace " +
                                                                  "'%s', please stop them before deleting datasets " +
                                                                  "in the namespace.",
                                                                namespaceId));
    }

    // Namespace data can be deleted. Revoke all privileges first
    authorizerInstantiatorService.get().enforce(namespaceId.toEntityId(), SecurityRequestContext.toPrincipal(),
                                                Action.ADMIN);
    try {
      dsFramework.deleteAllInstances(namespaceId);
    } catch (DatasetManagementException | IOException e) {
      LOG.warn("Error while deleting datasets in namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId, e);
    }
    LOG.debug("Deleted datasets in namespace '{}'.", namespaceId);
  }

  @Override
  public synchronized void updateProperties(Id.Namespace namespaceId, NamespaceMeta namespaceMeta) throws Exception {
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    authorizerInstantiatorService.get().enforce(namespaceId.toEntityId(), SecurityRequestContext.toPrincipal(),
                                                Action.ADMIN);
    NamespaceMeta metadata = nsStore.get(namespaceId);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder(metadata);

    if (namespaceMeta.getDescription() != null) {
      builder.setDescription(namespaceMeta.getDescription());
    }

    NamespaceConfig config = namespaceMeta.getConfig();
    if (config != null && !Strings.isNullOrEmpty(config.getSchedulerQueueName())) {
      builder.setSchedulerQueueName(config.getSchedulerQueueName());
    }

    nsStore.update(builder.build());
  }

  private boolean checkProgramsRunning(final NamespaceId namespaceId) {
    return runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program program) {
        return program.getNamespaceId().equals(namespaceId.getNamespace());
      }
    }, ProgramType.values());
  }

  private InstanceId createInstanceId(CConfiguration cConf) {
    String instanceName = cConf.get(Constants.INSTANCE_NAME);
    Preconditions.checkArgument(namespacePattern.matcher(instanceName).matches(),
                                "CDAP instance name specified by '%s' in cdap-site.xml should be alphanumeric " +
                                  "(underscores allowed). Its current invalid value is '%s'",
                                Constants.INSTANCE_NAME, instanceName);
    return new InstanceId(instanceName);
  }
}
