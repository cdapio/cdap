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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.security.AuthEnforce;
import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Admin for managing namespaces.
 */
public final class DefaultNamespaceAdmin implements NamespaceAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceAdmin.class);
  private static final Pattern NAMESPACE_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  private final NamespaceStore nsStore;
  private final DatasetFramework dsFramework;

  // Cannot have direct dependency on the following three resources
  // Otherwise there would be circular dependency
  // Use Provider to abstract out
  private final Provider<NamespaceResourceDeleter> resourceDeleter;
  private final Provider<ProgramRuntimeService> runtimeService;
  private final Provider<StorageProviderNamespaceAdmin> storageProviderNamespaceAdmin;
  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;
  private final InstanceId instanceId;
  private final Impersonator impersonator;
  private final CConfiguration cConf;
  private final LoadingCache<NamespaceId, NamespaceMeta> namespaceMetaCache;
  private final String masterShortUserName;

  @Inject
  DefaultNamespaceAdmin(NamespaceStore nsStore,
                        DatasetFramework dsFramework,
                        Provider<NamespaceResourceDeleter> resourceDeleter,
                        Provider<ProgramRuntimeService> runtimeService,
                        Provider<StorageProviderNamespaceAdmin> storageProviderNamespaceAdmin,
                        PrivilegesManager privilegesManager,
                        CConfiguration cConf,
                        Impersonator impersonator, AuthorizationEnforcer authorizationEnforcer,
                        AuthenticationContext authenticationContext) {
    this.resourceDeleter = resourceDeleter;
    this.nsStore = nsStore;
    this.dsFramework = dsFramework;
    this.runtimeService = runtimeService;
    this.privilegesManager = privilegesManager;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
    this.instanceId = createInstanceId(cConf);
    this.storageProviderNamespaceAdmin = storageProviderNamespaceAdmin;
    this.impersonator = impersonator;
    this.cConf = cConf;
    this.namespaceMetaCache = CacheBuilder.newBuilder().build(new CacheLoader<NamespaceId, NamespaceMeta>() {
      @Override
      public NamespaceMeta load(NamespaceId namespaceId) throws Exception {
        return fetchNamespaceMeta(namespaceId);
      }
    });
    String masterPrincipal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    try {
      if (SecurityUtil.isKerberosEnabled(cConf)) {
        this.masterShortUserName = new KerberosName(masterPrincipal).getShortName();
      } else {
        this.masterShortUserName = null;
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws NamespaceAlreadyExistsException if the specified namespace already exists
   */
  @Override
  @AuthEnforce(entities = "instanceId", enforceOn = InstanceId.class, actions = Action.ADMIN)
  public synchronized void create(final NamespaceMeta metadata) throws Exception {
    // TODO: CDAP-1427 - This should be transactional, but we don't support transactions on files yet
    Preconditions.checkArgument(metadata != null, "Namespace metadata should not be null.");
    NamespaceId namespace = metadata.getNamespaceId();
    if (exists(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }

    // if this namespace has custom mapping then validate the given custom mapping
    if (hasCustomMapping(metadata)) {
      validateCustomMapping(metadata);
    }

    // Namespace can be created. Check if the user is authorized now.
    Principal principal = authenticationContext.getPrincipal();
    privilegesManager.grant(namespace, principal, EnumSet.allOf(Action.class));
    // Also grant the user who will execute programs in this namespace all privileges on the namespace
    String executionUserName;
    if (SecurityUtil.isKerberosEnabled(cConf)) {
      ImpersonationInfo impersonationInfo = new ImpersonationInfo(metadata, cConf);
      String namespacePrincipal = impersonationInfo.getPrincipal();
      executionUserName = new KerberosName(namespacePrincipal).getShortName();
    } else {
      executionUserName = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    Principal executionUser = new Principal(executionUserName, Principal.PrincipalType.USER);
    privilegesManager.grant(namespace, executionUser, EnumSet.allOf(Action.class));

    // store the meta first in the namespace store because namespacedlocationfactory need to look up location
    // mapping from namespace config
    nsStore.create(metadata);

    try {
      impersonator.doAs(metadata, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          storageProviderNamespaceAdmin.get().create(metadata);
          return null;
        }
      });
    } catch (Throwable t) {
      // failed to create namespace in underlying storage so delete the namespace meta stored in the store earlier
      deleteNamespaceMeta(metadata.getNamespaceId());
      privilegesManager.revoke(namespace);
      throw new NamespaceCannotBeCreatedException(namespace, t);
    }
  }

  private void validateCustomMapping(NamespaceMeta metadata) throws Exception {
    for (NamespaceMeta existingNamespaceMeta : list()) {
      NamespaceConfig existingConfig = existingNamespaceMeta.getConfig();
      // if hbase namespace is provided validate no other existing namespace is mapped to it
      if (!Strings.isNullOrEmpty(metadata.getConfig().getHbaseNamespace()) &&
        metadata.getConfig().getHbaseNamespace().equals(existingConfig.getHbaseNamespace())) {
        throw new BadRequestException(String.format("A namespace '%s' already exists with the given " +
                                                                  "namespace mapping for hbase namespace '%s'",
                                                                existingNamespaceMeta.getName(),
                                                                existingConfig.getHbaseNamespace()));
      }
      // if hive database is provided validate no other existing namespace is mapped to it
      if (!Strings.isNullOrEmpty(metadata.getConfig().getHiveDatabase()) &&
        metadata.getConfig().getHiveDatabase().equals(existingConfig.getHiveDatabase())) {
        throw new BadRequestException(String.format("A namespace '%s' already exists with the given " +
                                                                  "namespace mapping for hive database '%s'",
                                                                existingNamespaceMeta.getName(),
                                                                existingConfig.getHiveDatabase()));
      }
      if (!Strings.isNullOrEmpty(metadata.getConfig().getRootDirectory())) {
        // check that the given root directory path is an absolute path
        validatePath(metadata.getName(), metadata.getConfig().getRootDirectory());
        // make sure that this new location is not same as some already mapped location or subdir of the existing
        // location or vice versa.
        if (hasSubDirRelationship(existingConfig.getRootDirectory(), metadata.getConfig().getRootDirectory())) {
          throw new BadRequestException(String.format("Failed to create namespace %s with custom " +
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
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NamespaceCannotBeDeletedException if the specified namespace cannot be deleted
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   */
  @Override
  @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
  public synchronized void delete(@Name("namespaceId") final NamespaceId namespaceId) throws Exception {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    NamespaceMeta namespaceMeta = get(namespaceId);

    if (checkProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
                                                  String.format("Some programs are currently running in namespace " +
                                                                  "'%s', please stop them before deleting namespace",
                                                                namespaceId));
    }

    LOG.info("Deleting namespace '{}'.", namespaceId);
    try {
      resourceDeleter.get().deleteResources(namespaceMeta);

      // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
      // create default namespace, and hence deleting it may cause undeterministic behavior.
      // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
      // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
      if (!NamespaceId.DEFAULT.equals(namespaceId)) {
        // Finally delete namespace from MDS and remove from cache
        deleteNamespaceMeta(namespaceId);

        // revoke privileges as the final step. This is done in the end, because if it is done before actual deletion,
        // and deletion fails, we may have a valid (or invalid) namespace in the system, that no one has privileges on,
        // so no one can clean up. This may result in orphaned privileges, which will be cleaned up by the create API
        // if the same namespace is successfully re-created.
        privilegesManager.revoke(namespaceId);
        LOG.info("Namespace '{}' deleted", namespaceId);
      } else {
        LOG.info("Keeping the '{}' namespace after removing all data.", NamespaceId.DEFAULT);
      }
    } catch (Exception e) {
      LOG.warn("Error while deleting namespace {}", namespaceId, e);
      throw new NamespaceCannotBeDeletedException(namespaceId, e);
    }
  }

  @Override
  @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
  public synchronized void deleteDatasets(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
    // TODO: CDAP-870, CDAP-1427: Delete should be in a single transaction.
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }

    if (checkProgramsRunning(namespaceId)) {
      throw new NamespaceCannotBeDeletedException(namespaceId,
                                                  String.format("Some programs are currently running in namespace " +
                                                                  "'%s', please stop them before deleting datasets " +
                                                                  "in the namespace.",
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
  public synchronized void updateProperties(NamespaceId namespaceId, NamespaceMeta namespaceMeta) throws Exception {
    if (!exists(namespaceId)) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    authorizationEnforcer.enforce(namespaceId, authenticationContext.getPrincipal(), Action.ADMIN);

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

    Set<String> difference = existingMeta.getConfig().getDifference(config);
    if (!difference.isEmpty()) {
      throw new BadRequestException(String.format("Mappings %s for namespace %s cannot be updated once the namespace " +
                                                    "is created.", difference, namespaceId));
    }
    nsStore.update(builder.build());
    // refresh the cache with new meta
    namespaceMetaCache.refresh(namespaceId);
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  @Override
  public List<NamespaceMeta> list() throws Exception {
    List<NamespaceMeta> namespaces = nsStore.list();
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return Lists.newArrayList(
      Iterables.filter(namespaces, new com.google.common.base.Predicate<NamespaceMeta>() {
        @Override
        public boolean apply(NamespaceMeta namespaceMeta) {
          return filter.apply(namespaceMeta.getNamespaceId());
        }
      })
    );
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   * @throws UnauthorizedException if the namespace is not authorized to the logged-user
   */
  @Override
  public NamespaceMeta get(NamespaceId namespaceId) throws Exception {
    NamespaceMeta namespaceMeta;
    try {
      namespaceMeta = namespaceMetaCache.get(namespaceId);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NamespaceNotFoundException || cause instanceof IOException ||
        cause instanceof UnauthorizedException) {
        throw (Exception) cause;
      }
      throw e;
    }
    Principal principal = authenticationContext.getPrincipal();
    // if the principal is same as cdap master principal skip the authorization check and just return the namespace
    // meta. See: CDAP-7387
    if (masterShortUserName != null && masterShortUserName.equals(principal.getName())) {
      return namespaceMeta;
    }

    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!filter.apply(namespaceId)) {
      throw new UnauthorizedException(principal, namespaceId);
    }
    return namespaceMeta;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specified namespace exists, false otherwise
   */
  @Override
  public boolean exists(NamespaceId namespaceId) throws Exception {
    try {
      // here we are not calling get(Id.Namespace namespaceId) method as we don't want authorization enforcement for
      // exists
      namespaceMetaCache.get(namespaceId);
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof NamespaceNotFoundException) {
        return false;
      }
      throw e;
    }
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
    Iterable<ProgramRuntimeService.RuntimeInfo> runtimeInfos =
      Iterables.filter(runtimeService.get().listAll(ProgramType.values()),
                       new com.google.common.base.Predicate<ProgramRuntimeService.RuntimeInfo>() {
                         @Override
                         public boolean apply(ProgramRuntimeService.RuntimeInfo info) {
                           return info.getProgramId().getNamespaceId().equals(namespaceId);
                         }
                       });
    return !Iterables.isEmpty(runtimeInfos);
  }

  private InstanceId createInstanceId(CConfiguration cConf) {
    String instanceName = cConf.get(Constants.INSTANCE_NAME);
    Preconditions.checkArgument(NAMESPACE_PATTERN.matcher(instanceName).matches(),
                                "CDAP instance name specified by '%s' in cdap-site.xml should be alphanumeric " +
                                  "(underscores allowed). Its current invalid value is '%s'",
                                Constants.INSTANCE_NAME, instanceName);
    return new InstanceId(instanceName);
  }

  /**
   * Deletes the namespace meta and also invalidates the cache
   * @param namespaceId of namespace whose meta needs to be deleted
   */
  private void deleteNamespaceMeta(NamespaceId namespaceId) {
    nsStore.delete(namespaceId);
    namespaceMetaCache.invalidate(namespaceId);
  }
}
