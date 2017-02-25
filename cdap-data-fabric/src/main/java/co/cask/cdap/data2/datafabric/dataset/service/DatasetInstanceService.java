/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.common.HttpErrorStatusProvider;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * Handles dataset instance management calls.
 */
public class DatasetInstanceService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceService.class);

  private final DatasetTypeService typeService;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final ExploreFacade exploreFacade;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final OwnerAdmin ownerAdmin;
  private final LoadingCache<DatasetId, DatasetMeta> metaCache;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final PrivilegesManager privilegesManager;
  private final AuthenticationContext authenticationContext;

  private AuditPublisher auditPublisher;

  @VisibleForTesting
  @Inject
  public DatasetInstanceService(DatasetTypeService typeService, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, ExploreFacade exploreFacade,
                                NamespaceQueryAdmin namespaceQueryAdmin, OwnerAdmin ownerAdmin,
                                AuthorizationEnforcer authorizationEnforcer, PrivilegesManager privilegesManager,
                                AuthenticationContext authenticationContext) {
    this.opExecutorClient = opExecutorClient;
    this.typeService = typeService;
    this.instanceManager = instanceManager;
    this.exploreFacade = exploreFacade;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.ownerAdmin = ownerAdmin;
    this.metaCache = CacheBuilder.newBuilder().build(
      new CacheLoader<DatasetId, DatasetMeta>() {
        @Override
        public DatasetMeta load(DatasetId datasetId) throws Exception {
          return getFromMds(datasetId);
        }
      }
    );
    this.authorizationEnforcer = authorizationEnforcer;
    this.privilegesManager = privilegesManager;
    this.authenticationContext = authenticationContext;
  }

  @VisibleForTesting
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  /**
   * Lists all dataset instances in a namespace. If perimeter security and authorization are enabled, only returns the
   * dataset instances that the current user has access to.
   *
   * @param namespace the namespace to list datasets for
   * @return the dataset instances in the provided namespace
   * @throws NotFoundException if the namespace was not found
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists
   */
  Collection<DatasetSpecification> list(final NamespaceId namespace) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    ensureNamespaceExists(namespace);
    Collection<DatasetSpecification> datasets = instanceManager.getAll(namespace);
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    return Lists.newArrayList(Iterables.filter(datasets, new com.google.common.base.Predicate<DatasetSpecification>() {
      @Override
      public boolean apply(DatasetSpecification spec) {
        return filter.apply(namespace.dataset(spec.getName()));
      }
    }));
  }

  /**
   * Gets a dataset instance.
   *
   * @param instance instance to get
   * @param owners the {@link EntityId entities} that will be using the dataset instance
   * @return the dataset instance's {@link DatasetMeta}
   * @throws NotFoundException if either the namespace or dataset instance is not found,
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the current user does not
   *  have any privileges on the #instance
   */
  DatasetMeta get(final DatasetId instance, List<? extends EntityId> owners) throws Exception {
    // Application Deployment first makes a call to the dataset service to check if the instance already exists with
    // a different type. To make sure that that call responds with the right exceptions if necessary, first fetch the
    // meta from the cache and throw appropriate exceptions if necessary.
    DatasetMeta datasetMeta;
    try {
      datasetMeta = metaCache.get(instance);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if ((cause instanceof Exception) && (cause instanceof HttpErrorStatusProvider)) {
         throw (Exception) cause;
      }
      throw e;
    }
    // Only return the above datasetMeta if authorization succeeds
    ensureAccess(instance);
    return datasetMeta;
  }

  /**
   * Read the dataset meta data (instance and type) from MDS.
   */
  private DatasetMeta getFromMds(DatasetId instance) throws Exception {
    // TODO: CDAP-3901 add back namespace existence check
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new NotFoundException(instance);
    }
    spec = DatasetsUtil.fixOriginalProperties(spec);

    DatasetTypeId datasetTypeId = instance.getParent().datasetType(spec.getType());
    DatasetTypeMeta typeMeta = getTypeInfo(instance.getParent(), spec.getType());
    if (typeMeta == null) {
      // TODO: This shouldn't happen unless CDAP is in an invalid state - maybe give different error
      throw new NotFoundException(datasetTypeId);
    }
    // for system dataset do not look up owner information in store as we know that it will be null.
    // Also, this is required for CDAP to start, because initially we don't want to look up owner admin
    // (causing its own lookup) as the SystemDatasetInitiator.getDataset is called when CDAP starts
    String ownerPrincipal = null;
    if (!NamespaceId.SYSTEM.equals(instance.getNamespaceId())) {
      ownerPrincipal = ownerAdmin.getImpersonationPrincipal(instance);
    }
    return new DatasetMeta(spec, typeMeta, null, ownerPrincipal);
  }

  /**
   * Return the original properties of a dataset instance, that is, the properties with which the dataset was
   * created or last reconfigured.
   * @param instance the id of the dataset
   * @return The original properties as stored in the dataset's spec, or if they are not available, a best effort
   *   to derive the original properties from the top-level properties of the spec
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the current user does not
   *   have any privileges on the #instance
   */
  Map<String, String> getOriginalProperties(DatasetId instance) throws Exception {
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new NotFoundException(instance);
    }
    // Only return the properties if authorization succeeds
    ensureAccess(instance);
    return DatasetsUtil.fixOriginalProperties(spec).getOriginalProperties();
  }

  /**
   * Creates a dataset instance.
   *
   * @param namespaceId the namespace to create the dataset instance in
   * @param name the name of the new dataset instance
   * @param props the properties for the new dataset instance
   * @throws NamespaceNotFoundException if the specified namespace was not found
   * @throws DatasetAlreadyExistsException if a dataset with the same name already exists
   * @throws DatasetTypeNotFoundException if the dataset type was not found
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the current user does not
   *  have {@link Action#WRITE} privilege on the #instance's namespace
   */
  void create(String namespaceId, String name, DatasetInstanceConfiguration props) throws Exception {
    NamespaceId namespace = ConversionHelpers.toNamespaceId(namespaceId);
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(namespace, principal, Action.WRITE);

    ensureNamespaceExists(namespace);

    DatasetId datasetId = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    DatasetSpecification existing = instanceManager.get(datasetId);
    if (existing != null) {
      throw new DatasetAlreadyExistsException(datasetId);
    }

    DatasetTypeMeta typeMeta = getTypeInfo(namespace, props.getTypeName());
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(ConversionHelpers.toDatasetTypeId(namespace, props.getTypeName()));
    }

    // It is now determined that a new dataset will be created. First grant privileges, then create the dataset.
    // If creation fails, revoke the granted privileges. This ensures that just like delete, there may be orphaned
    // privileges in rare scenarios, but there can never be orphaned datasets.
    // If the dataset previously existed and was deleted, but revoking privileges somehow failed, there may be orphaned
    // privileges for the dataset. Revoke them first, so no users unintentionally get privileges on the dataset.
    privilegesManager.revoke(datasetId);
    // grant all privileges on the dataset to be created
    privilegesManager.grant(datasetId, principal, EnumSet.allOf(Action.class));

    LOG.info("Creating dataset {}.{}, type name: {}, properties: {}",
             namespaceId, name, props.getTypeName(), props.getProperties());

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    try {
      String ownerPrincipal = props.getOwnerPrincipal();
      // Store the owner principal first if one was provided since it will be used to impersonate while creating
      // dataset's files/tables in the underlying storage
      // it has already been established that the dataset doesn't exists so no need to check if an owner principal
      // exists or not
      if (ownerPrincipal != null) {
        KerberosPrincipalId owner = new KerberosPrincipalId(ownerPrincipal);
        ownerAdmin.add(datasetId, owner);
      }
      try {
        DatasetSpecification spec = opExecutorClient.create(datasetId, typeMeta,
                                                            DatasetProperties.builder()
                                                              .addAll(props.getProperties())
                                                              .setDescription(props.getDescription())
                                                              .build());
        instanceManager.add(namespace, spec);
        metaCache.invalidate(datasetId);
        publishAudit(datasetId, AuditType.CREATE);

        // Enable explore
        enableExplore(datasetId, spec, props);
      } catch (Exception e) {
        // there was a problem in creating the dataset instance so delete the owner if it got added earlier
        ownerAdmin.delete(datasetId); // safe to call for entities which does not have an owner too
        throw e;
      }
    } catch (Exception e) {
      // there was a problem in creating the dataset instance so revoke the privileges
      privilegesManager.revoke(datasetId);
      throw e;
    }
  }

  /**
   * Updates an existing Dataset specification properties.
   * {@link DatasetInstanceConfiguration} is constructed based on request and the Dataset instance is updated.
   *
   * @param instance the dataset instance
   * @param properties the dataset properties to be used
   * @throws NamespaceNotFoundException if the specified namespace was not found
   * @throws DatasetNotFoundException if the dataset was not found
   * @throws DatasetTypeNotFoundException if the type of the existing dataset was not found
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the current user does not
   *  have {@link Action#ADMIN} privilege on the #instance
   */
  void update(DatasetId instance, Map<String, String> properties) throws Exception {
    ensureNamespaceExists(instance.getParent());
    DatasetSpecification existing = instanceManager.get(instance);
    if (existing == null) {
      throw new DatasetNotFoundException(instance);
    }

    LOG.info("Update dataset {}, properties: {}", instance.getEntityName(), ConversionHelpers.toJson(properties));
    authorizationEnforcer.enforce(instance, authenticationContext.getPrincipal(), Action.ADMIN);

    DatasetTypeMeta typeMeta = getTypeInfo(instance.getParent(), existing.getType());
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(
        ConversionHelpers.toDatasetTypeId(instance.getParent(), existing.getType()));
    }

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetProperties datasetProperties = DatasetProperties.of(properties);
    DatasetSpecification spec = opExecutorClient.update(instance, typeMeta, datasetProperties, existing);
    instanceManager.add(instance.getParent(), spec);
    metaCache.invalidate(instance);

    updateExplore(instance, datasetProperties, existing, spec);
    publishAudit(instance, AuditType.UPDATE);
  }

  /**
   * Drops the specified dataset instance.
   *
   * @param instance the {@link DatasetId} to drop
   * @throws NamespaceNotFoundException if the namespace was not found
   * @throws DatasetNotFoundException if the dataset instance was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the current user does not
   *  have {@link Action#ADMIN} privileges on the #instance
   */
  void drop(DatasetId instance) throws Exception {
    ensureNamespaceExists(instance.getParent());
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new DatasetNotFoundException(instance);
    }

    authorizationEnforcer.enforce(instance, authenticationContext.getPrincipal(), Action.ADMIN);
    dropDataset(instance, spec);
  }

  /**
   * Drops all datasets in the given namespace. If authorization is turned on, only datasets that the current
   * principal that has {@link Action#ADMIN} privilege will be deleted
   *
   * @param namespaceId namespace to operate on
   * @throws Exception if it fails to delete dataset
   * @return the set of {@link DatasetId} that get deleted
   */
  Set<DatasetId> dropAll(NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId);
    Principal principal = authenticationContext.getPrincipal();

    Set<DatasetId> datasets = new HashSet<>();
    for (DatasetSpecification spec : instanceManager.getAll(namespaceId)) {
      try {
        DatasetId datasetId = namespaceId.dataset(spec.getName());
        authorizationEnforcer.enforce(datasetId, principal, Action.ADMIN);
        dropDataset(datasetId, spec);
        datasets.add(datasetId);
      } catch (UnauthorizedException e) {
        // It's ok to be not authorized. Just skip the deletion.
      }
    }

    return datasets;
  }

  /**
   * Executes an admin operation on a dataset instance.
   *
   * @param instance the instance to execute the admin operation on
   * @param method the type of admin operation to execute
   * @return the {@link DatasetAdminOpResponse} from the HTTP handler
   * @throws NamespaceNotFoundException if the requested namespace was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the current user does not
   *  have -
   *  <ol>
   *    <li>{@link Action#ADMIN} privileges on the #instance (for "drop" or "truncate") </li>
   *    <li>any privileges on the #instance (for "exists")</li>
   *  <ol>
   */
  DatasetAdminOpResponse executeAdmin(DatasetId instance, String method) throws Exception {
    ensureNamespaceExists(instance.getParent());

    Object result = null;

    // NOTE: one cannot directly call create and drop, instead this should be called thru
    //       POST/DELETE @ /data/datasets/{instance-id}. Because we must create/drop metadata for these at same time
    Principal principal = authenticationContext.getPrincipal();
    switch (method) {
      case "exists":
        ensureAccess(instance);
        result = opExecutorClient.exists(instance);
        break;
      case "truncate":
        if (instanceManager.get(instance) == null) {
          throw new DatasetNotFoundException(instance);
        }
        authorizationEnforcer.enforce(instance, principal, Action.ADMIN);
        opExecutorClient.truncate(instance);
        publishAudit(instance, AuditType.TRUNCATE);
        break;
      case "upgrade":
        if (instanceManager.get(instance) == null) {
          throw new DatasetNotFoundException(instance);
        }
        authorizationEnforcer.enforce(instance, principal, Action.ADMIN);
        opExecutorClient.upgrade(instance);
        publishAudit(instance, AuditType.UPDATE);
        break;
      default:
        throw new HandlerException(HttpResponseStatus.NOT_FOUND, "Invalid admin operation: " + method);
    }

    return new DatasetAdminOpResponse(result, null);
  }

  /**
   * Finds the {@link DatasetTypeMeta} for the specified dataset type name.
   * Search order - first in the specified namespace, then in the 'system' namespace from defaultModules
   *
   * @param namespaceId {@link NamespaceId} for the specified namespace
   * @param typeName the name of the dataset type to search
   * @return {@link DatasetTypeMeta} for the type if found in either the specified namespace or in the system namespace,
   * null otherwise.
   * TODO: This may need to move to a util class eventually
   */
  @Nullable
  private DatasetTypeMeta getTypeInfo(NamespaceId namespaceId, String typeName) throws Exception {
    DatasetTypeId datasetTypeId = ConversionHelpers.toDatasetTypeId(namespaceId, typeName);
    try {
      return typeService.getType(datasetTypeId);
    } catch (DatasetTypeNotFoundException e) {
      try {
        // Type not found in the instance's namespace. Now try finding it in the system namespace
        DatasetTypeId systemDatasetTypeId = ConversionHelpers.toDatasetTypeId(NamespaceId.SYSTEM, typeName);
        return typeService.getType(systemDatasetTypeId);
      } catch (DatasetTypeNotFoundException exnWithSystemNS) {
        // if it's not found in system namespace, throw the original exception with the correct namespace
        throw e;
      }
    }
  }

  /**
   * Drops a dataset.
   *
   * @param spec specification of dataset to be dropped.
   * @throws Exception on error.
   */
  private void dropDataset(DatasetId instance, DatasetSpecification spec) throws Exception {
    LOG.info("Deleting dataset {}.{}", instance.getNamespace(), instance.getEntityName());

    disableExplore(instance, spec);

    if (!instanceManager.delete(instance)) {
      throw new DatasetNotFoundException(instance);
    }
    metaCache.invalidate(instance);

    DatasetTypeMeta typeMeta = getTypeInfo(instance.getParent(), spec.getType());
    if (typeMeta == null) {
      throw new DatasetNotFoundException(instance);
    }
    opExecutorClient.drop(instance, typeMeta, spec);

    publishAudit(instance, AuditType.DELETE);
    // revoke privileges as the final step. This is done in the end, because if it is done before actual deletion, and
    // deletion fails, we may have a valid (or invalid) dataset in the system, that no one has privileges on, so no one
    // can clean up. This may result in orphaned privileges, which will be cleaned up by the create API if the same
    // dataset is successfully re-created.
    privilegesManager.revoke(instance);
    // deletes the owner principal for the entity if one was stored during creation
    ownerAdmin.delete(instance);
  }

  private void disableExplore(DatasetId datasetInstance, DatasetSpecification spec) {
    // Disable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-1393
    try {
      exploreFacade.disableExploreDataset(datasetInstance, spec);
    } catch (Exception e) {
      LOG.error("Cannot disable Explore for dataset instance {}", datasetInstance, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
    }
  }

  private void enableExplore(DatasetId datasetInstance, DatasetSpecification spec,
                             DatasetInstanceConfiguration creationProperties) {
    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-1393
    try {
      exploreFacade.enableExploreDataset(datasetInstance, spec, false);
    } catch (Exception e) {
      LOG.error("Cannot enable Explore for dataset instance {} of type {} with properties {}",
                datasetInstance, creationProperties.getTypeName(), creationProperties.getProperties(), e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
    }
  }

  private void updateExplore(DatasetId datasetInstance, DatasetProperties creationProperties,
                             DatasetSpecification oldSpec, DatasetSpecification newSpec) {
    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-1393
    try {
      exploreFacade.updateExploreDataset(datasetInstance, oldSpec, newSpec);
    } catch (Exception e) {
      LOG.error("Cannot update Explore for dataset instance {} with old properties {} and new properties {}",
                datasetInstance, oldSpec.getOriginalProperties(), creationProperties.getProperties(), e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
    }
  }

  /**
   * Throws an exception if the specified namespace is not the system namespace and does not exist
   */
  private void ensureNamespaceExists(NamespaceId namespaceId) throws Exception {
    if (!NamespaceId.SYSTEM.equals(namespaceId)) {
      if (!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
    }
  }

  private void publishAudit(DatasetId datasetInstance, AuditType auditType) {
    // TODO: Add properties to Audit Payload (CDAP-5220)
    AuditPublishers.publishAudit(auditPublisher, datasetInstance, auditType, AuditPayload.EMPTY_PAYLOAD);
  }

  /**
   * Ensures that the logged-in user has a {@link Action privilege} on the specified dataset instance.
   *
   * @param datasetId the {@link DatasetId} to check for privileges
   * @throws UnauthorizedException if the logged in user has no {@link Action privileges} on the specified dataset
   */
  private void ensureAccess(DatasetId datasetId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!filter.apply(datasetId)) {
      throw new UnauthorizedException(principal, datasetId);
    }
  }
}

