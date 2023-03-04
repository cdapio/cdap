/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.DatasetAlreadyExistsException;
import io.cdap.cdap.common.DatasetNotFoundException;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.common.HandlerException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data2.audit.AuditPublisher;
import io.cdap.cdap.data2.audit.AuditPublishers;
import io.cdap.cdap.data2.datafabric.dataset.DatasetsUtil;
import io.cdap.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetCreationResponse;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import io.cdap.cdap.data2.metadata.system.DelegateSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.system.SystemMetadata;
import io.cdap.cdap.data2.metadata.system.SystemMetadataWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.audit.AuditPayload;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles dataset instance management calls.
 */
public class DatasetInstanceService {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceService.class);

  private final DatasetTypeService authorizationDatasetTypeService;
  private final DatasetTypeService noAuthDatasetTypeService;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final OwnerAdmin ownerAdmin;
  private final LoadingCache<DatasetId, DatasetMeta> metaCache;
  private final AccessEnforcer accessEnforcer;
  private final AuthenticationContext authenticationContext;

  private AuditPublisher auditPublisher;
  private MetadataServiceClient metadataServiceClient;

  @VisibleForTesting
  @Inject
  public DatasetInstanceService(DatasetTypeService authorizationDatasetTypeService,
      @Named(DataSetServiceModules.NOAUTH_DATASET_TYPE_SERVICE)
          DatasetTypeService noAuthDatasetTypeService,
      DatasetInstanceManager instanceManager,
      DatasetOpExecutor opExecutorClient,
      NamespaceQueryAdmin namespaceQueryAdmin, OwnerAdmin ownerAdmin,
      AccessEnforcer accessEnforcer,
      AuthenticationContext authenticationContext,
      MetadataServiceClient metadataServiceClient) {
    this.opExecutorClient = opExecutorClient;
    this.authorizationDatasetTypeService = authorizationDatasetTypeService;
    this.noAuthDatasetTypeService = noAuthDatasetTypeService;
    this.instanceManager = instanceManager;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.ownerAdmin = ownerAdmin;
    this.metadataServiceClient = metadataServiceClient;
    this.metaCache = CacheBuilder.newBuilder().build(
        new CacheLoader<DatasetId, DatasetMeta>() {
          @Override
          public DatasetMeta load(DatasetId datasetId) throws Exception {
            return getFromMds(datasetId);
          }
        }
    );
    this.accessEnforcer = accessEnforcer;
    this.authenticationContext = authenticationContext;
  }

  @VisibleForTesting
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  /**
   * Lists all dataset instances in a namespace.
   *
   * @param namespace the namespace to list datasets for
   * @return the dataset instances in the provided namespace
   * @throws NotFoundException if the namespace was not found
   * @throws IOException if there is a problem in making an HTTP request to check if the
   *     namespace exists
   */
  Collection<DatasetSpecification> list(final NamespaceId namespace) throws Exception {
    ensureNamespaceExists(namespace);
    accessEnforcer.enforceOnParent(EntityType.DATASET, namespace,
        authenticationContext.getPrincipal(),
        StandardPermission.LIST);
    return instanceManager.getAll(namespace);
  }

  /**
   * Lists all dataset instances in a namespace having specified properties.
   *
   * @param namespace the namespace to list datasets for
   * @param properties the dataset properties
   * @return the dataset instances in the provided namespace satisfying the given properties. If no
   *     property is specified all instances are returned
   * @throws NotFoundException if the namespace was not found
   * @throws IOException if there is a problem in making an HTTP request to check if the
   *     namespace exists
   */
  Collection<DatasetSpecification> list(final NamespaceId namespace, Map<String, String> properties)
      throws Exception {
    ensureNamespaceExists(namespace);
    accessEnforcer.enforceOnParent(EntityType.DATASET, namespace,
        authenticationContext.getPrincipal(),
        StandardPermission.LIST);
    return instanceManager.get(namespace, properties);
  }

  /**
   * Gets the metadata for a dataset instance.
   *
   * @param instance instance to get
   * @return the dataset instance's {@link DatasetMeta}
   * @throws NotFoundException if either the namespace or dataset instance is not found,
   * @throws IOException if there is a problem in making an HTTP request to check if the
   *     namespace exists
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the
   *     current user does not have any privileges on the #instance
   */
  DatasetMeta get(final DatasetId instance) throws Exception {
    // ensure user has correct privileges before getting the meta if the dataset is not a system dataset
    if (!DatasetsUtil.isSystemDatasetInUserNamespace(instance)) {
      LOG.trace("Authorizing GET for dataset {}", instance.getDataset());
      accessEnforcer.enforce(instance, authenticationContext.getPrincipal(),
          StandardPermission.GET);
      LOG.trace("Authorized GET for dataset {}", instance.getDataset());
    }
    // Application Deployment first makes a call to the dataset service to check if the instance already exists with
    // a different type. To make sure that that call responds with the right exceptions if necessary, first fetch the
    // meta from the cache and throw appropriate exceptions if necessary.
    DatasetMeta datasetMeta;
    try {
      LOG.trace("Retrieving instance metadata from cache for dataset {}", instance.getDataset());
      datasetMeta = metaCache.get(instance);
      LOG.trace("Retrieved instance metadata from cache for dataset {}", instance.getDataset());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if ((cause instanceof Exception) && (cause instanceof HttpErrorStatusProvider)) {
        throw (Exception) cause;
      }
      throw e;
    }
    return datasetMeta;
  }

  /**
   * Read the dataset meta data (instance and type) from MDS.
   *
   * Note this method cannot be called to create dataset instance, since it does not have
   * enforcement on the dataset type.
   */
  private DatasetMeta getFromMds(DatasetId instance) throws Exception {
    // TODO: CDAP-3901 add back namespace existence check
    LOG.trace("Retrieving instance metadata from MDS for dataset {}", instance.getDataset());
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new NotFoundException(instance);
    }
    LOG.trace("Retrieved instance metadata from MDS for dataset {}", instance.getDataset());
    spec = DatasetsUtil.fixOriginalProperties(spec);

    DatasetTypeId datasetTypeId = instance.getParent().datasetType(spec.getType());
    // by pass the auth check for dataset type when the operation is not creation
    DatasetTypeMeta typeMeta = getTypeInfo(instance.getParent(), spec.getType(), true);
    if (typeMeta == null) {
      // TODO: This shouldn't happen unless CDAP is in an invalid state - maybe give different error
      throw new NotFoundException(datasetTypeId);
    }
    // for system dataset do not look up owner information in store as we know that it will be null.
    // Also, this is required for CDAP to start, because initially we don't want to look up owner admin
    // (causing its own lookup) as the SystemDatasetInitiator.getDataset is called when CDAP starts
    String ownerPrincipal = null;
    if (!NamespaceId.SYSTEM.equals(instance.getNamespaceId())) {
      LOG.trace("Retrieving owner principal for dataset {}", instance.getDataset());
      ownerPrincipal = ownerAdmin.getOwnerPrincipal(instance);
      LOG.trace("Retrieved owner principal for dataset {}", instance.getDataset());
    }
    return new DatasetMeta(spec, typeMeta, null, ownerPrincipal);
  }

  /**
   * Return the original properties of a dataset instance, that is, the properties with which the
   * dataset was created or last reconfigured.
   *
   * @param instance the id of the dataset
   * @return The original properties as stored in the dataset's spec, or if they are not available,
   *     a best effort to derive the original properties from the top-level properties of the spec
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the
   *     current user does not have any privileges on the #instance
   */
  Map<String, String> getOriginalProperties(DatasetId instance) throws Exception {
    // Only return the properties if authorization succeeds
    if (!DatasetsUtil.isSystemDatasetInUserNamespace(instance)) {
      accessEnforcer.enforce(instance, authenticationContext.getPrincipal(),
          StandardPermission.GET);
    }
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new NotFoundException(instance);
    }
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
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the
   *     current user does not have {@link StandardPermission#UPDATE} privilege on the #instance's
   *     namespace
   */
  void create(String namespaceId, String name, DatasetInstanceConfiguration props)
      throws Exception {
    NamespaceId namespace = ConversionHelpers.toNamespaceId(namespaceId);
    DatasetId datasetId = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    Principal requestingUser = authenticationContext.getPrincipal();
    String ownerPrincipal = props.getOwnerPrincipal();

    // need to enforce on the principal id if impersonation is involved
    KerberosPrincipalId effectiveOwner = SecurityUtil.getEffectiveOwner(ownerAdmin, namespace,
        ownerPrincipal);
    if (DatasetsUtil.isUserDataset(datasetId)) {
      LOG.trace("Authorizing impersonation for dataset {}", name);
      if (effectiveOwner != null) {
        accessEnforcer.enforce(effectiveOwner, requestingUser, AccessPermission.SET_OWNER);
      }
      accessEnforcer.enforce(datasetId, requestingUser, StandardPermission.CREATE);
      LOG.trace("Authorized impersonation for dataset {}", name);
    }

    LOG.trace("Ensuring existence of namespace {} for dataset {}", namespace, name);
    ensureNamespaceExists(namespace);
    LOG.trace("Ensured existence of namespace {} for dataset {}", namespace, name);

    LOG.trace("Retrieving instance metadata from MDS for dataset {}", name);
    DatasetSpecification existing = instanceManager.get(datasetId);
    if (existing != null) {
      throw new DatasetAlreadyExistsException(datasetId);
    }
    LOG.trace("Retrieved instance metadata from MDS for dataset {}", name);

    // for creation, we need enforcement for dataset type for user dataset, but bypass for system datasets
    DatasetTypeMeta typeMeta = getTypeInfo(namespace, props.getTypeName(),
        !DatasetsUtil.isUserDataset(datasetId));
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(
          ConversionHelpers.toDatasetTypeId(namespace, props.getTypeName()));
    }

    LOG.info("Creating dataset {}.{}, type name: {}, properties: {}",
        namespaceId, name, props.getTypeName(), props.getProperties());

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    // Store the owner principal first if one was provided since it will be used to impersonate while creating
    // dataset's files/tables in the underlying storage
    // it has already been established that the dataset doesn't exists so no need to check if an owner principal
    // exists or not
    if (ownerPrincipal != null) {
      LOG.trace("Adding owner for dataset {}", name);
      KerberosPrincipalId owner = new KerberosPrincipalId(ownerPrincipal);
      ownerAdmin.add(datasetId, owner);
      LOG.trace("Added owner {} for dataset {}", owner, name);
    }
    try {
      DatasetProperties datasetProperties = DatasetProperties.builder()
          .addAll(props.getProperties())
          .setDescription(props.getDescription())
          .build();

      LOG.trace("Calling op executor service to configure dataset {}", name);
      DatasetCreationResponse response = opExecutorClient.create(datasetId, typeMeta,
          datasetProperties);
      LOG.trace("Received spec and metadata from op executor service for dataset {}: {}", name,
          response);

      LOG.trace("Adding instance metadata for dataset {}", name);
      DatasetSpecification spec = response.getSpec();
      instanceManager.add(namespace, spec);
      LOG.trace("Added instance metadata for dataset {}", name);
      metaCache.invalidate(datasetId);

      LOG.trace("Publishing audit for creation of dataset {}", name);
      publishAudit(datasetId, AuditType.CREATE);
      LOG.trace("Published audit for creation of dataset {}", name);
      SystemMetadata metadata = response.getMetadata();
      LOG.trace("Publishing system metadata for creation of dataset {}: {}", name, metadata);
      publishMetadata(datasetId, metadata);
      LOG.trace("Published system metadata for creation of dataset {}", name);
    } catch (Exception e) {
      // there was a problem in creating the dataset instance so delete the owner if it got added earlier
      ownerAdmin.delete(datasetId); // safe to call for entities which does not have an owner too
      throw e;
    }
  }

  /**
   * Updates an existing Dataset specification properties. {@link DatasetInstanceConfiguration} is
   * constructed based on request and the Dataset instance is updated.
   *
   * @param instance the dataset instance
   * @param properties the dataset properties to be used
   * @throws NamespaceNotFoundException if the specified namespace was not found
   * @throws DatasetNotFoundException if the dataset was not found
   * @throws DatasetTypeNotFoundException if the type of the existing dataset was not found
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the
   *     current user does not have {@link StandardPermission#UPDATE} privilege on the #instance
   */
  void update(DatasetId instance, Map<String, String> properties) throws Exception {
    ensureNamespaceExists(instance.getParent());
    Principal requestingUser = authenticationContext.getPrincipal();
    if (!DatasetsUtil.isSystemDatasetInUserNamespace(instance)) {
      accessEnforcer.enforce(instance, requestingUser, StandardPermission.UPDATE);
    }
    DatasetSpecification existing = instanceManager.get(instance);
    if (existing == null) {
      throw new DatasetNotFoundException(instance);
    }

    LOG.info("Update dataset {}, properties: {}", instance.getEntityName(),
        ConversionHelpers.toJson(properties));

    // by pass the auth check for dataset type when the operation is not creation
    DatasetTypeMeta typeMeta = getTypeInfo(instance.getParent(), existing.getType(), true);
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(
          ConversionHelpers.toDatasetTypeId(instance.getParent(), existing.getType()));
    }

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetProperties datasetProperties = DatasetProperties.of(properties);
    DatasetCreationResponse response = opExecutorClient.update(instance, typeMeta,
        datasetProperties, existing);
    DatasetSpecification spec = response.getSpec();
    instanceManager.add(instance.getParent(), spec);
    metaCache.invalidate(instance);

    publishAudit(instance, AuditType.UPDATE);
    publishMetadata(instance, response.getMetadata());
  }

  /**
   * Drops the specified dataset.
   *
   * @param datasetId the {@link DatasetId} to drop
   * @throws NamespaceNotFoundException if the namespace was not found
   * @throws DatasetNotFoundException if the dataset datasetId was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the
   *     current user does not have {@link StandardPermission#DELETE} privileges on the dataset
   */
  void drop(DatasetId datasetId) throws Exception {
    Principal requestingUser = authenticationContext.getPrincipal();
    if (!DatasetsUtil.isSystemDatasetInUserNamespace(datasetId)) {
      accessEnforcer.enforce(datasetId, requestingUser, StandardPermission.DELETE);
    }
    ensureNamespaceExists(datasetId.getParent());
    DatasetSpecification spec = instanceManager.get(datasetId);
    if (spec == null) {
      throw new DatasetNotFoundException(datasetId);
    }

    dropDataset(datasetId, spec);
  }

  /**
   * Drops all datasets in the given namespace. If authorization is turned on, only datasets that
   * the current principal that has {@link StandardPermission#DELETE} privilege will be deleted
   *
   * @param namespaceId namespace to operate on
   * @throws Exception if it fails to delete dataset
   */
  void dropAll(NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId);
    Principal requestingUser = authenticationContext.getPrincipal();

    Map<DatasetId, DatasetSpecification> datasets = new HashMap<>();
    for (DatasetSpecification spec : instanceManager.getAll(namespaceId)) {
      DatasetId datasetId = namespaceId.dataset(spec.getName());
      if (DatasetsUtil.isUserDataset(datasetId)) {
        accessEnforcer.enforce(datasetId, requestingUser, StandardPermission.DELETE);
      }
      datasets.put(datasetId, spec);
    }

    // auth check passed, we can start deleting the datasets
    for (DatasetId datasetId : datasets.keySet()) {
      dropDataset(datasetId, datasets.get(datasetId));
    }
  }

  /**
   * Executes an admin operation on a dataset.
   *
   * @param datasetId the datasetId to execute the admin operation on
   * @param method the type of admin operation to execute
   * @return the {@link DatasetAdminOpResponse} from the HTTP handler
   * @throws NamespaceNotFoundException if the requested namespace was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   * @throws UnauthorizedException if perimeter security and authorization are enabled, and the
   *     current user does not have
  - *     <ol>
   *       <li>{@link StandardPermission#DELETE} privileges on the dataset for "truncate" </li>
   *       <li>{@link StandardPermission#UPDATE} privileges on the dataset for "upgrade" </li>
   *       <li>read privileges on the dataset for "exists"</li>
   *     <ol>
   */
  DatasetAdminOpResponse executeAdmin(DatasetId datasetId, String method) throws Exception {
    ensureNamespaceExists(datasetId.getParent());

    Object result = null;

    // NOTE: one cannot directly call create and drop, instead this should be called thru
    //       POST/DELETE @ /data/datasets/{datasetId-id}. Because we must create/drop metadata for these at same time
    Principal principal = authenticationContext.getPrincipal();
    switch (method) {
      case "exists":
        // ensure the user has some privilege on the dataset datasetId if it is not system dataset
        if (!DatasetsUtil.isSystemDatasetInUserNamespace(datasetId)) {
          accessEnforcer.enforce(datasetId, principal, StandardPermission.GET);
        }
        result = opExecutorClient.exists(datasetId);
        break;
      case "truncate":
        if (!DatasetsUtil.isSystemDatasetInUserNamespace(datasetId)) {
          accessEnforcer.enforce(datasetId, principal, StandardPermission.DELETE);
        }
        if (instanceManager.get(datasetId) == null) {
          throw new DatasetNotFoundException(datasetId);
        }
        opExecutorClient.truncate(datasetId);
        publishAudit(datasetId, AuditType.TRUNCATE);
        break;
      case "upgrade":
        if (!DatasetsUtil.isSystemDatasetInUserNamespace(datasetId)) {
          accessEnforcer.enforce(datasetId, principal, StandardPermission.UPDATE);
        }
        if (instanceManager.get(datasetId) == null) {
          throw new DatasetNotFoundException(datasetId);
        }
        opExecutorClient.upgrade(datasetId);
        publishAudit(datasetId, AuditType.UPDATE);
        break;
      default:
        throw new HandlerException(HttpResponseStatus.NOT_FOUND,
            "Invalid admin operation: " + method);
    }

    return new DatasetAdminOpResponse(result, null);
  }

  /**
   * Finds the {@link DatasetTypeMeta} for the specified dataset type name. Search order - first in
   * the specified namespace, then in the 'system' namespace from defaultModules
   *
   * @param namespaceId {@link NamespaceId} for the specified namespace
   * @param typeName the name of the dataset type to search
   * @param byPassCheck a flag which determines whether to check privilege for the dataset type
   * @return {@link DatasetTypeMeta} for the type if found in either the specified namespace or in
   *     the system namespace, null otherwise.
   *         TODO: This may need to move to a util class eventually
   */
  @Nullable
  private DatasetTypeMeta getTypeInfo(NamespaceId namespaceId, String typeName, boolean byPassCheck)
      throws Exception {
    DatasetTypeId datasetTypeId = ConversionHelpers.toDatasetTypeId(namespaceId, typeName);
    try {
      LOG.trace("Retrieving metadata from mds for dataset type {} with authorization: {}", typeName,
          byPassCheck);
      DatasetTypeMeta meta = byPassCheck ? noAuthDatasetTypeService.getType(datasetTypeId) :
          authorizationDatasetTypeService.getType(datasetTypeId);
      LOG.trace("Retrieved metadata from mds for dataset type {}", typeName);
      return meta;
    } catch (DatasetTypeNotFoundException | UnauthorizedException e) {
      try {
        // Type not found in the instance's namespace. Now try finding it in the system namespace
        LOG.trace("Retrieving metadata from mds for system dataset type {}", typeName);
        DatasetTypeId systemDatasetTypeId = ConversionHelpers.toDatasetTypeId(NamespaceId.SYSTEM,
            typeName);
        LOG.trace("Retrieved metadata from mds for system dataset type {}", typeName);
        return noAuthDatasetTypeService.getType(systemDatasetTypeId);
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

    if (!instanceManager.delete(instance)) {
      throw new DatasetNotFoundException(instance);
    }
    metaCache.invalidate(instance);

    // by pass the auth check for dataset type when the operation is not creation
    DatasetTypeMeta typeMeta = getTypeInfo(instance.getParent(), spec.getType(), true);
    if (typeMeta == null) {
      throw new DatasetNotFoundException(instance);
    }
    opExecutorClient.drop(instance, typeMeta, spec);

    // Remove metadata for the dataset
    LOG.trace("Removing metadata for dataset {}", instance);
    metadataServiceClient.drop(new MetadataMutation.Drop(instance.toMetadataEntity()));
    LOG.trace("Removed metadata for dataset {}", instance);

    publishAudit(instance, AuditType.DELETE);
    // deletes the owner principal for the entity if one was stored during creation
    ownerAdmin.delete(instance);
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
    AuditPublishers.publishAudit(auditPublisher, datasetInstance, auditType,
        AuditPayload.EMPTY_PAYLOAD);
  }

  private void publishMetadata(DatasetId dataset, SystemMetadata metadata) {
    if (metadata != null && !metadata.isEmpty()) {
      SystemMetadataWriter metadataWriter = new DelegateSystemMetadataWriter(metadataServiceClient,
          dataset, metadata);
      metadataWriter.write();
    }
  }
}

