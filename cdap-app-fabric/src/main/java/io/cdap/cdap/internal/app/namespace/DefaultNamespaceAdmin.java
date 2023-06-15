/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.namespace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.NamespaceCannotBeCreatedException;
import io.cdap.cdap.common.NamespaceCannotBeDeletedException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.security.AuthEnforce;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.tethering.PeerInfo;
import io.cdap.cdap.internal.tethering.TetheringStore;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.impersonation.ImpersonationUtils;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.store.NamespaceStore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin for managing namespaces.
 */
public final class DefaultNamespaceAdmin implements NamespaceAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);
  private static final String DEFAULT = "default";

  private final NamespaceStore nsStore;
  private final Store store;
  private final DatasetFramework dsFramework;
  private final MetricsCollectionService metricsCollectionService;

  // Cannot have direct dependency on the following three resources
  // Otherwise there would be circular dependency
  // Use Provider to abstract out
  private final Provider<NamespaceResourceDeleter> resourceDeleter;
  private final Provider<StorageProviderNamespaceAdmin> storageProviderNamespaceAdmin;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;
  private final Impersonator impersonator;
  private final LoadingCache<NamespaceId, NamespaceMeta> namespaceMetaCache;
  private final String masterShortUserName;
  private final TetheringStore tetheringStore;
  private final CConfiguration cConf;

  /**
   * Constructs the {@link DefaultNamespaceAdmin} using specified parameters.
   */
  @Inject
  @VisibleForTesting
  public DefaultNamespaceAdmin(NamespaceStore nsStore,
      Store store,
      DatasetFramework dsFramework,
      MetricsCollectionService metricsCollectionService,
      Provider<NamespaceResourceDeleter> resourceDeleter,
      Provider<StorageProviderNamespaceAdmin> storageProviderNamespaceAdmin,
      CConfiguration cConf, Impersonator impersonator, AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      TetheringStore tetheringStore) {
    this.resourceDeleter = resourceDeleter;
    this.nsStore = nsStore;
    this.store = store;
    this.dsFramework = dsFramework;
    this.metricsCollectionService = metricsCollectionService;
    this.authenticationContext = authenticationContext;
    this.accessEnforcer = accessEnforcer;
    this.storageProviderNamespaceAdmin = storageProviderNamespaceAdmin;
    this.impersonator = impersonator;
    this.namespaceMetaCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<NamespaceId, NamespaceMeta>() {
          @Override
          public NamespaceMeta load(NamespaceId namespaceId) throws Exception {
            return fetchNamespaceMeta(namespaceId);
          }
        });
    this.masterShortUserName = AuthorizationUtil.getEffectiveMasterUser(cConf);
    this.tetheringStore = tetheringStore;
    this.cConf = cConf;
  }

  private boolean existsWithoutAuth(NamespaceId namespaceId) throws Exception {
    try {
      namespaceMetaCache.get(namespaceId);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof NamespaceNotFoundException) {
        return false;
      }
      throw e;
    }
    return true;
  }

  /**
   * Creates a new namespace.
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws NamespaceAlreadyExistsException if the specified namespace already exists
   */
  @Override
  public synchronized void create(final NamespaceMeta metadata) throws Exception {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    NamespaceId namespace = metadata.getNamespaceId();
    Principal requestingUser = authenticationContext.getPrincipal();
    accessEnforcer.enforce(namespace, requestingUser, StandardPermission.CREATE);
    // It does not make sense to check permissions for getting a namespace when it may not exist.
    if (existsWithoutAuth(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }

    // need to enforce on the principal id if impersonation is involved
    String ownerPrincipal = metadata.getConfig().getPrincipal();
    if (ownerPrincipal != null) {
      accessEnforcer.enforce(new KerberosPrincipalId(ownerPrincipal), requestingUser,
          AccessPermission.SET_OWNER);
    }

    // If this namespace has custom mapping then validate the given custom mapping
    if (hasCustomMapping(metadata)) {
      validateCustomMapping(metadata);
    }

    // check that the user has configured either both or none of the following configuration: principal and keytab URI
    boolean hasValidKerberosConf = false;
    if (metadata.getConfig() != null) {
      String configuredPrincipal = metadata.getConfig().getPrincipal();
      String configuredKeytabUri = metadata.getConfig().getKeytabURI();
      if ((!Strings.isNullOrEmpty(configuredPrincipal) && Strings.isNullOrEmpty(
          configuredKeytabUri))
          || (Strings.isNullOrEmpty(configuredPrincipal) && !Strings.isNullOrEmpty(
          configuredKeytabUri))) {
        throw new BadRequestException(
            String.format(
                "Either both or none of the following two configurations must be configured. "
                    + "Configured principal: %s, Configured keytabURI: %s",
                configuredPrincipal, configuredKeytabUri));
      }
      hasValidKerberosConf = true;
    }

    // store the meta first in the namespace store because namespacedLocationFactory needs to look up location
    // mapping from namespace config
    nsStore.create(metadata);
    try {
      UserGroupInformation ugi;
      if (NamespaceId.DEFAULT.equals(namespace)) {
        ugi = UserGroupInformation.getCurrentUser();
      } else {
        ugi = impersonator.getUGI(namespace);
      }
      ImpersonationUtils.doAs(ugi, (Callable<Void>) () -> {
        storageProviderNamespaceAdmin.get().create(metadata);
        return null;
      });

      // if needed, run master environment specific logic
      MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
      if (masterEnv != null) {
        if (cConf.getBoolean(Constants.Namespace.NAMESPACE_CREATION_HOOK_ENABLED)) {
          masterEnv.onNamespaceCreation(namespace.getNamespace(),
              metadata.getConfig().getConfigs());
        } else {
          masterEnv.createIdentity(NamespaceId.DEFAULT.getNamespace(),
              getIdentity(namespace.getNamespaceId()));
        }
      }
    } catch (Throwable t) {
      LOG.error(String.format("Failed to create namespace '%s'", namespace.getNamespace()), t);
      try {
        resourceDeleter.get().deleteResources(metadata);
      } catch (Exception e) {
        LOG.error(String.format("Failed to delete resources for namespace '%s'",
            namespace.getNamespace()), e);
        t.addSuppressed(e);
      }
      try {
        // failed to create namespace in underlying storage so delete the namespace meta stored in the store earlier
        deleteNamespaceMeta(metadata.getNamespaceId());
      } catch (Exception e) {
        LOG.error(
            String.format("Failed to delete metadata for namespace '%s'", namespace.getNamespace()),
            e);
        t.addSuppressed(e);
      }
      throw new NamespaceCannotBeCreatedException(namespace, t);
    }
    emitNamespaceCountMetric();
    LOG.info("Namespace {} created with meta {}", metadata.getNamespaceId(), metadata);
  }

  private void validateCustomMapping(NamespaceMeta metadata) throws Exception {
    for (NamespaceMeta existingNamespaceMeta : list()) {
      NamespaceConfig existingConfig = existingNamespaceMeta.getConfig();
      // if hbase namespace is provided validate no other existing namespace is mapped to it
      if (!Strings.isNullOrEmpty(metadata.getConfig().getHbaseNamespace())
          && metadata.getConfig().getHbaseNamespace().equals(existingConfig.getHbaseNamespace())) {
        throw new BadRequestException(
            String.format("A namespace '%s' already exists with the given "
                    + "namespace mapping for hbase namespace '%s'",
                existingNamespaceMeta.getName(),
                existingConfig.getHbaseNamespace()));
      }
      // if hive database is provided validate no other existing namespace is mapped to it
      if (!Strings.isNullOrEmpty(metadata.getConfig().getHiveDatabase())
          && metadata.getConfig().getHiveDatabase().equals(existingConfig.getHiveDatabase())) {
        throw new BadRequestException(
            String.format("A namespace '%s' already exists with the given "
                    + "namespace mapping for hive database '%s'",
                existingNamespaceMeta.getName(),
                existingConfig.getHiveDatabase()));
      }
      if (!Strings.isNullOrEmpty(metadata.getConfig().getRootDirectory())) {
        // check that the given root directory path is an absolute path
        validatePath(metadata.getName(), metadata.getConfig().getRootDirectory());
        // make sure that this new location is not same as some already mapped location or subdir of the existing
        // location or vice versa.
        if (hasSubDirRelationship(existingConfig.getRootDirectory(),
            metadata.getConfig().getRootDirectory())) {
          throw new BadRequestException(String.format("Failed to create namespace %s with custom "
                  + "location %s. A namespace '%s' already exists "
                  + "with location '%s' and these two locations are "
                  + "have a subdirectory relationship.",
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
    return !Strings.isNullOrEmpty(existingDir)
        && (Paths.get(newDir).startsWith(existingDir) || Paths.get(existingDir).startsWith(newDir));
  }

  private boolean hasCustomMapping(NamespaceMeta metadata) {
    NamespaceConfig config = metadata.getConfig();
    return !(Strings.isNullOrEmpty(config.getRootDirectory()) && Strings.isNullOrEmpty(
        config.getHbaseNamespace())
        && Strings.isNullOrEmpty(config.getHiveDatabase()));
  }

  private void validatePath(String namespace, String rootDir) throws IOException {
    // a custom location was provided
    // check that its an absolute path
    File customLocation = new File(rootDir);
    if (!customLocation.isAbsolute()) {
      throw new IOException(String.format(
          "Cannot create the namespace '%s' with the given custom location %s. Custom location must be absolute path.",
          namespace, customLocation));
    }
  }

  /**
   * Deletes the specified namespace.
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NamespaceCannotBeDeletedException if the specified namespace cannot be deleted
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   */
  @Override
  @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, permissions = StandardPermission.DELETE)
  public synchronized void delete(@Name("namespaceId") final NamespaceId namespaceId)
      throws Exception {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    NamespaceMeta namespaceMeta = get(namespaceId);

    if (checkProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
          String.format("Some programs are currently running in namespace "
                  + "'%s', please stop them before deleting namespace",
              namespaceId));
    }

    List<String> tetheredPeers = getTetheredPeersUsingNamespace(namespaceId);
    if (!tetheredPeers.isEmpty()) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
          String.format("Namespace '%s' is used in tethering connections "
                  + "with peers: %s. Delete tethering connections "
                  + "before deleting the namespace",
              namespaceId,
              tetheredPeers));
    }

    LOG.info("Deleting namespace '{}'.", namespaceId);
    try {
      // if needed, run master environment specific logic if it is a non-default namespace (see below for more info)
      MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
      if (cConf.getBoolean(Constants.Namespace.NAMESPACE_CREATION_HOOK_ENABLED)
          && masterEnv != null && !NamespaceId.DEFAULT.equals(namespaceId)) {
        masterEnv.onNamespaceDeletion(namespaceId.getNamespace(),
            namespaceMeta.getConfig().getConfigs());
      }

      resourceDeleter.get().deleteResources(namespaceMeta);

      // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
      // create default namespace, and hence deleting it may cause undeterministic behavior.
      // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
      // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
      if (!NamespaceId.DEFAULT.equals(namespaceId)) {
        // Finally delete namespace from MDS and remove from cache
        deleteNamespaceMeta(namespaceId);

        LOG.info("Namespace '{}' deleted", namespaceId);
      } else {
        LOG.info("Keeping the '{}' namespace after removing all data.", NamespaceId.DEFAULT);
      }
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId, e);
    }
    emitNamespaceCountMetric();
  }

  @Override
  public synchronized void deleteDatasets(@Name("namespaceId") NamespaceId namespaceId)
      throws Exception {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    if (checkProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
          String.format("Some programs are currently running in namespace "
                  + "'%s', please stop them before deleting datasets "
                  + "in the namespace.",
              namespaceId));
    }
    try {
      dsFramework.deleteAllInstances(namespaceId);
    } catch (DatasetManagementException | IOException e) {
      LOG.warn("Error while deleting datasets in namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId, e);
    }
    LOG.debug("Deleted datasets in namespace '{}'.", namespaceId);
  }

  @Override
  public synchronized void updateProperties(NamespaceId namespaceId, NamespaceMeta namespaceMeta)
      throws Exception {
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    accessEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(),
        StandardPermission.UPDATE);

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

    if (config != null && config.getKeytabURI() != null) {
      String keytabUri = config.getKeytabURI();
      if (keytabUri.isEmpty()) {
        throw new BadRequestException("Cannot update keytab URI with an empty URI.");
      }
      String existingKeytabUri = existingMeta.getConfig().getKeytabUriWithoutVersion();
      if (existingKeytabUri == null) {
        throw new BadRequestException(
            "Cannot update keytab URI since there is no existing principal or keytab URI.");
      }
      if (keytabUri.equals(existingKeytabUri)) {
        // The given keytab URI is the same as the existing one, but the content of the keytab file might be changed.
        // Increment the keytab URI version so that the cache will reload content in the updated keytab file.
        builder.incrementKeytabUriVersion();
      } else {
        builder.setKeytabUriWithoutVersion(keytabUri);
        // clear keytab URI version
        builder.setKeytabUriVersion(0);
      }
    }

    Set<String> difference = existingMeta.getConfig().getDifference(config);
    if (!difference.isEmpty()) {
      throw new BadRequestException(
          String.format("Mappings %s for namespace %s cannot be updated once the namespace "
              + "is created.", difference, namespaceId));
    }
    NamespaceMeta updatedMeta = builder.build();
    nsStore.update(updatedMeta);
    // refresh the cache with new meta
    namespaceMetaCache.refresh(namespaceId);
    LOG.info("Namespace {} updated with meta {}", namespaceId, updatedMeta);
  }

  /**
   * Lists all namespaces.
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  @Override
  public List<NamespaceMeta> list() throws Exception {
    List<NamespaceMeta> namespaces = nsStore.list();
    final Principal principal = authenticationContext.getPrincipal();

    //noinspection ConstantConditions
    return AuthorizationUtil.isVisible(namespaces, accessEnforcer, principal,
        NamespaceMeta::getNamespaceId,
        input -> principal.getName().equals(input.getConfig().getPrincipal()));
  }

  /**
   * Gets details of a namespace.
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   * @throws UnauthorizedException if the namespace is not authorized to the logged-user
   */
  @Override
  public NamespaceMeta get(NamespaceId namespaceId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();

    UnauthorizedException lastUnauthorizedException = null;
    // if the principal is not same as cdap master principal do the authorization check. Otherwise, skip the auth check
    // See: CDAP-7387
    if (masterShortUserName == null || !masterShortUserName.equals(principal.getName())) {
      try {
        accessEnforcer.enforce(namespaceId, principal, StandardPermission.GET);
      } catch (UnauthorizedException e) {
        lastUnauthorizedException = e;
      }
    }

    NamespaceMeta namespaceMeta = null;
    try {
      namespaceMeta = namespaceMetaCache.get(namespaceId);
    } catch (Exception e) {
      if (lastUnauthorizedException == null) {
        Throwable cause = e.getCause();
        if (cause instanceof NamespaceNotFoundException || cause instanceof IOException
            || cause instanceof UnauthorizedException) {
          throw (Exception) cause;
        }
        throw e;
      }
    }

    // If the requesting user is same as namespace owner, we do not care about if the user is authorized or not
    if (namespaceMeta != null && principal.getName()
        .equals(namespaceMeta.getConfig().getPrincipal())) {
      return namespaceMeta;
    }

    if (lastUnauthorizedException != null) {
      throw lastUnauthorizedException;
    }
    return namespaceMeta;
  }

  /**
   * Checks if the specified namespace exists.
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @returns true, if the specified namespace exists, false otherwise
   */
  @Override
  public boolean exists(NamespaceId namespaceId) throws Exception {
    try {
      // here we are not calling get(Id.Namespace namespaceId) method as we don't want authorization enforcement for
      // exists
      get(namespaceId);
      return true;
    } catch (NamespaceNotFoundException e) {
      return false;
    }
  }

  @Override
  public String getIdentity(NamespaceId namespaceId) {
    if (cConf.getBoolean(Constants.Namespace.NAMESPACE_CREATION_HOOK_ENABLED)) {
      // if namespace creation hook is enabled, use the default identity.
      return DEFAULT;
    }
    return NamespaceAdmin.super.getIdentity(namespaceId);
  }

  @VisibleForTesting
  Map<NamespaceId, NamespaceMeta> getCache() {
    return namespaceMetaCache.asMap();
  }

  private NamespaceMeta fetchNamespaceMeta(NamespaceId namespaceId) throws Exception {
    NamespaceMeta ns = nsStore.get(namespaceId);
    if (ns == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    return ns;
  }

  private boolean checkProgramsRunning(final NamespaceId namespaceId) {
    return !store.getActiveRuns(namespaceId).isEmpty();
  }

  private List<String> getTetheredPeersUsingNamespace(NamespaceId namespaceId) throws IOException {
    return tetheringStore.getPeers()
        .stream()
        .filter(p -> p.getMetadata().getNamespaceAllocations().stream()
            .anyMatch(na -> na.getNamespace().equals(namespaceId.getNamespace())))
        .map(PeerInfo::getName)
        .collect(Collectors.toList());
  }

  /**
   * Deletes the namespace meta and also invalidates the cache.
   *
   * @param namespaceId of namespace whose meta needs to be deleted
   */
  private void deleteNamespaceMeta(NamespaceId namespaceId) {
    nsStore.delete(namespaceId);
    namespaceMetaCache.invalidate(namespaceId);
  }

  /**
   * Emit the namespace count metric.
   */
  private void emitNamespaceCountMetric() {
    metricsCollectionService.getContext(Collections.emptyMap())
        .gauge(Constants.Metrics.Program.NAMESPACE_COUNT,
            nsStore.getNamespaceCount());
  }
}
