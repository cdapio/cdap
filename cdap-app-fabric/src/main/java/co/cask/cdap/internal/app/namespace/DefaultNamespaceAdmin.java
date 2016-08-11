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
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.config.DashboardStore;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.service.ExploreException;
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
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Admin for managing namespaces.
 */
public final class DefaultNamespaceAdmin extends DefaultNamespaceQueryAdmin implements NamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);

  private final Store store;
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
  private final Authorizer authorizer;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final InstanceId instanceId;
  private final StorageProviderNamespaceAdmin storageProviderNamespaceAdmin;
  private final Impersonator impersonator;
  private final Pattern namespacePattern = Pattern.compile("[a-zA-Z0-9_]+");

  @Inject
  DefaultNamespaceAdmin(Store store, NamespaceStore nsStore, PreferencesStore preferencesStore,
                        DashboardStore dashboardStore, DatasetFramework dsFramework,
                        ProgramRuntimeService runtimeService, QueueAdmin queueAdmin, StreamAdmin streamAdmin,
                        MetricStore metricStore, Scheduler scheduler,
                        ApplicationLifecycleService applicationLifecycleService,
                        ArtifactRepository artifactRepository,
                        AuthorizerInstantiator authorizerInstantiator,
                        CConfiguration cConf, StorageProviderNamespaceAdmin storageProviderNamespaceAdmin,
                        Impersonator impersonator, AuthorizationEnforcer authorizationEnforcer,
                        AuthenticationContext authenticationContext) {
    super(nsStore, authorizationEnforcer, authenticationContext);
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
    this.store = store;
    this.preferencesStore = preferencesStore;
    this.dashboardStore = dashboardStore;
    this.dsFramework = dsFramework;
    this.runtimeService = runtimeService;
    this.scheduler = scheduler;
    this.metricStore = metricStore;
    this.applicationLifecycleService = applicationLifecycleService;
    this.artifactRepository = artifactRepository;
    this.authorizer = authorizerInstantiator.get();
    this.authorizationEnforcer = authorizationEnforcer;
    this.instanceId = createInstanceId(cConf);
    this.storageProviderNamespaceAdmin = storageProviderNamespaceAdmin;
    this.impersonator = impersonator;
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws NamespaceAlreadyExistsException if the specified namespace already exists
   */
  @Override
  public synchronized void create(final NamespaceMeta metadata) throws Exception {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    NamespaceId namespace = new NamespaceId(metadata.getName());
    if (exists(namespace.toId())) {
      throw new NamespaceAlreadyExistsException(namespace.toId());
    }

    // if this namespace has custom mapping then validate the given custom mapping
    if (hasCustomMapping(metadata)) {
      validateCustomMapping(metadata);
    }

    // Namespace can be created. Check if the user is authorized now.
    Principal principal = authenticationContext.getPrincipal();
    // Skip authorization enforcement for the system user and the default namespace, so the DefaultNamespaceEnsurer
    // thread can successfully create the default namespace
    if (!(Principal.SYSTEM.equals(principal) && NamespaceId.DEFAULT.equals(namespace))) {
      authorizationEnforcer.enforce(instanceId, principal, Action.ADMIN);
      authorizer.grant(namespace, principal, ImmutableSet.of(Action.ALL));
    }

    // store the meta first in the namespace store because namespacedlocationfactory need to look up location
    // mapping from namespace config
    nsStore.create(metadata);

    try {
      impersonator.doAs(metadata, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          storageProviderNamespaceAdmin.create(metadata);
          return null;
        }
      });
    } catch (IOException | ExploreException | SQLException e) {
      // failed to create namespace in underlying storage so delete the namespace meta stored in the store earlier
      nsStore.delete(metadata.getNamespaceId().toId());
      if (!(Principal.SYSTEM.equals(principal) && NamespaceId.DEFAULT.equals(namespace))) {
        authorizer.revoke(namespace);
      }
      throw new NamespaceCannotBeCreatedException(namespace.toId(), e);
    }
  }

  private void validateCustomMapping(NamespaceMeta metadata) throws Exception {
    for (NamespaceMeta existingNamespaceMeta : list()) {
      NamespaceConfig existingConfig = existingNamespaceMeta.getConfig();
      // if hbase namespace is provided validate no other existing namespace is mapped to it
      if (!Strings.isNullOrEmpty(metadata.getConfig().getHbaseNamespace()) &&
        metadata.getConfig().getHbaseNamespace().equals(existingConfig.getHbaseNamespace())) {
        throw new NamespaceAlreadyExistsException(existingNamespaceMeta.getNamespaceId().toId(),
                                                  String.format("A namespace '%s' already exists with the given " +
                                                                  "namespace mapping for hbase namespace '%s'",
                                                                existingNamespaceMeta.getName(),
                                                                existingConfig.getHbaseNamespace()));
      }
      // if hive database is provided validate no other existing namespace is mapped to it
      if (!Strings.isNullOrEmpty(metadata.getConfig().getHiveDatabase()) &&
        metadata.getConfig().getHiveDatabase().equals(existingConfig.getHiveDatabase())) {
        throw new NamespaceAlreadyExistsException(existingNamespaceMeta.getNamespaceId().toId(),
                                                  String.format("A namespace '%s' already exists with the given " +
                                                                  "namespace mapping for hive database '%s'",
                                                                existingNamespaceMeta.getName(),
                                                                existingConfig.getHiveDatabase()));
      }
      if (!Strings.isNullOrEmpty(metadata.getConfig().getRootDirectory())) {
        // check that the given root directory path is an absolute path
        validatePath(metadata);
        // make sure that this new location is not same as some already mapped location or subdir of the existing
        // location or vice versa.
        if (hasSubDirRelationship(existingConfig.getRootDirectory(), metadata.getConfig().getRootDirectory())) {
          throw new NamespaceAlreadyExistsException(existingNamespaceMeta.getNamespaceId().toId(),
                                                    String.format("Failed to create namespace %s with custom " +
                                                                    "location %s. A namespace '%s' already exists " +
                                                                    "with location '%s' and these two locations are " +
                                                                    "have a subdirectory relationship.",
                                                                  metadata.getName(),
                                                                  metadata.getConfig().getRootDirectory(),
                                                                  existingNamespaceMeta.getName(),
                                                                  existingConfig.getRootDirectory()));
        }
      }
    }
  }

  private boolean hasSubDirRelationship(@Nullable String existingDir, String newDir) {
    // only check for subdir if the existing namespace dir is custom mapped in which case this will not be null
    return !Strings.isNullOrEmpty(existingDir) &&
      (Paths.get(newDir).startsWith(existingDir) || Paths.get(existingDir).startsWith(newDir));
  }

  private boolean hasCustomMapping(NamespaceMeta metadata) {
    NamespaceConfig config = metadata.getConfig();
    return !(Strings.isNullOrEmpty(config.getRootDirectory()) && Strings.isNullOrEmpty(config.getHbaseNamespace()) &&
      Strings.isNullOrEmpty(config.getHiveDatabase()));
  }

  private void validatePath(NamespaceMeta namespaceMeta) throws IOException {
    // a custom location was provided
    // check that its an absolute path
    File customLocation = new File(namespaceMeta.getConfig().getRootDirectory());
    if (!customLocation.isAbsolute()) {
      throw new IOException(String.format(
        "Cannot create the namespace '%s' with the given custom location %s. Custom location must be absolute path.",
        namespaceMeta.getName(), customLocation));
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

    authorizationEnforcer.enforce(namespace, authenticationContext.getPrincipal(), Action.ADMIN);

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
        try {
          impersonator.doAs(namespace, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              // Delete namespace in storage providers
              storageProviderNamespaceAdmin.delete(namespaceId.toEntityId());
              return null;
            }
          });
        } finally {
          // Finally delete namespace from MDS
          nsStore.delete(namespaceId);
        }
      }
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId, e);
    } finally {
      authorizer.revoke(namespace);
    }
    LOG.info("All data for namespace '{}' deleted.", namespaceId);

    // revoke privileges as the final step. This is done in the end, because if it is done before actual deletion, and
    // deletion fails, we may have a valid (or invalid) namespace in the system, that no one has privileges on,
    // so no one can clean up. This may result in orphaned privileges, which will be cleaned up by the create API
    // if the same namespace is successfully re-created.
    authorizer.revoke(namespace);
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
    authorizationEnforcer.enforce(namespaceId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);
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
    authorizationEnforcer.enforce(namespaceId.toEntityId(), authenticationContext.getPrincipal(), Action.ADMIN);

    NamespaceMeta existingMeta = nsStore.get(namespaceId);
    // Already ensured that namespace exists, so namespace meta should not be null
    Preconditions.checkNotNull(existingMeta);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder(existingMeta);

    if (namespaceMeta.getDescription() != null) {
      builder.setDescription(namespaceMeta.getDescription());
    }

    NamespaceConfig config = namespaceMeta.getConfig();
    if (config != null && !Strings.isNullOrEmpty(config.getSchedulerQueueName())) {
      builder.setSchedulerQueueName(config.getSchedulerQueueName());
    }

    // we already checked namespace existence so the meta cannot be null here
    if (config != null && config.getRootDirectory() != null) {
      // if a root directory was given for update and it's not same as existing one throw exception
      if (!config.getRootDirectory().equals(existingMeta.getConfig().getRootDirectory())) {
        throw new BadRequestException(String.format("Updates to %s are not allowed. Cannot update from %s to %s.",
                                                    NamespaceConfig.ROOT_DIRECTORY,
                                                    existingMeta.getConfig().getRootDirectory(),
                                                    config.getRootDirectory()));
      }

      if (config.getHbaseNamespace() != null
        && (!config.getHbaseNamespace().equals(existingMeta.getConfig().getHbaseNamespace()))) {
        throw new BadRequestException(String.format("Updates to %s are not allowed. Cannot update from %s to %s.",
                                                    NamespaceConfig.HBASE_NAMESPACE,
                                                    existingMeta.getConfig().getHbaseNamespace(),
                                                    config.getHbaseNamespace()));
      }
    }

    if (config != null && config.getHiveDatabase() != null) {
      // if a hive database was given for update and it's not same as existing one throw exception
      if (!config.getHiveDatabase().equals(existingMeta.getConfig().getHiveDatabase())) {
        throw new BadRequestException(String.format("Updates to %s are not allowed. Cannot update from %s to %s.",
                                                    NamespaceConfig.HIVE_DATABASE,
                                                    existingMeta.getConfig().getHiveDatabase(),
                                                    config.getHiveDatabase()));
      }
    }

    nsStore.update(builder.build());
  }

  private boolean checkProgramsRunning(final NamespaceId namespaceId) {
    Iterable<ProgramRuntimeService.RuntimeInfo> runtimeInfos =
      Iterables.filter(runtimeService.listAll(ProgramType.values()),
                       new Predicate<ProgramRuntimeService.RuntimeInfo>() {
      @Override
      public boolean apply(ProgramRuntimeService.RuntimeInfo info) {
        return info.getProgramId().getNamespaceId().equals(namespaceId.getNamespace());
      }
    });
    return !Iterables.isEmpty(runtimeInfos);
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
