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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * Handles dataset instance management calls.
 */
public class DatasetInstanceService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceService.class);

  private final DatasetTypeManager typeManager;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final ExploreFacade exploreFacade;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  private AuditPublisher auditPublisher;

  private final LoadingCache<Id.DatasetInstance, DatasetMeta> metaCache;


  @Inject
  public DatasetInstanceService(DatasetTypeManager typeManager, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, ExploreFacade exploreFacade, CConfiguration conf,
                                NamespaceQueryAdmin namespaceQueryAdmin) {
    this.opExecutorClient = opExecutorClient;
    this.typeManager = typeManager;
    this.instanceManager = instanceManager;
    this.exploreFacade = exploreFacade;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.metaCache = CacheBuilder.newBuilder().build(
      new CacheLoader<Id.DatasetInstance, DatasetMeta>() {
        @Override
        public DatasetMeta load(Id.DatasetInstance datasetId) throws Exception {
          return getFromMds(datasetId);
        }
      }
    );
  }

  @SuppressWarnings("unused")
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
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists.
   */
  public Collection<DatasetSpecification> list(Id.Namespace namespace) throws Exception {
    // Throws NamespaceNotFoundException if the namespace does not exist
    ensureNamespaceExists(namespace);
    return instanceManager.getAll(namespace);
  }

  /**
   * Gets a dataset instance.
   *
   * @param instance instance to get
   * @param owners the {@link Id}s that will be using the dataset instance
   * @return the dataset instance's {@link DatasetMeta}
   * @throws NotFoundException if either the namespace or dataset instance is not found,
   * @throws IOException if there is a problem in making an HTTP request to check if the namespace exists.
   */
  public DatasetMeta get(Id.DatasetInstance instance, List<? extends Id> owners) throws Exception {
    try {
      return metaCache.get(instance);
    } catch (ExecutionException e) {
      if ((e.getCause() instanceof Exception) && (e.getCause() instanceof HttpErrorStatusProvider)) {
         throw (Exception) e.getCause();
      }
      throw e;
    }
  }

  /**
   * Read the dataset meta data (instance and type) from MDS.
   */
  private DatasetMeta getFromMds(Id.DatasetInstance instance) throws Exception {
    // TODO: CDAP-3901 add back namespace existence check
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new NotFoundException(instance);
    }
    spec = DatasetsUtil.fixOriginalProperties(spec);

    Id.DatasetType datasetTypeId = Id.DatasetType.from(instance.getNamespace(), spec.getType());
    DatasetTypeMeta typeMeta = getTypeInfo(instance.getNamespace(), spec.getType());
    if (typeMeta == null) {
      // TODO: This shouldn't happen unless CDAP is in an invalid state - maybe give different error
      throw new NotFoundException(datasetTypeId);
    }
    return new DatasetMeta(spec, typeMeta, null);
  }

  /**
   * Return the original properties of a dataset instance, that is, the properties with which the dataset was
   * created or last reconfigured.
   * @param instance the id of the dataset
   * @return The original properties as stored in the dataset's spec, or if they are not available, a best effort
   *   to derive the original properties from the top-level properties of the spec.
   */
  public Map<String, String> getOriginalProperties(Id.DatasetInstance instance) throws Exception {
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
   * @throws Exception if something went wrong
   */
  public void create(String namespaceId, String name, DatasetInstanceConfiguration props) throws Exception {
    // Throws NamespaceNotFoundException if the namespace does not exist
    Id.Namespace namespace = ConversionHelpers.toNamespaceId(namespaceId);
    ensureNamespaceExists(namespace);

    Id.DatasetInstance newInstance = ConversionHelpers.toDatasetInstanceId(namespaceId, name);
    DatasetSpecification existing = instanceManager.get(newInstance);
    if (existing != null) {
      throw new DatasetAlreadyExistsException(newInstance);
    }

    DatasetTypeMeta typeMeta = getTypeInfo(namespace, props.getTypeName());
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(ConversionHelpers.toDatasetTypeId(namespace, props.getTypeName()));
    }

    LOG.info("Creating dataset {}.{}, type name: {}, properties: {}",
             namespaceId, name, props.getTypeName(), props.getProperties());

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec = opExecutorClient.create(newInstance, typeMeta,
                                                        DatasetProperties.builder()
                                                          .addAll(props.getProperties())
                                                          .setDescription(props.getDescription())
                                                          .build());
    instanceManager.add(namespace, spec);
    metaCache.invalidate(newInstance);
    publishAudit(newInstance, AuditType.CREATE);

    // Enable explore
    enableExplore(newInstance, props);
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
   */
  public void update(Id.DatasetInstance instance, Map<String, String> properties) throws Exception {
    // Throws NamespaceNotFoundException if the namespace does not exist
    ensureNamespaceExists(instance.getNamespace());
    DatasetSpecification existing = instanceManager.get(instance);
    if (existing == null) {
      throw new DatasetNotFoundException(instance);
    }

    LOG.info("Update dataset {}, properties: {}", instance.getId(), ConversionHelpers.toJson(properties));

    DatasetTypeMeta typeMeta = getTypeInfo(instance.getNamespace(), existing.getType());
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(
        ConversionHelpers.toDatasetTypeId(instance.getNamespace(), existing.getType()));
    }

    disableExplore(instance);

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec = opExecutorClient.update(instance, typeMeta, DatasetProperties.of(properties), existing);
    instanceManager.add(instance.getNamespace(), spec);
    metaCache.invalidate(instance);

    DatasetInstanceConfiguration creationProperties =
      new DatasetInstanceConfiguration(existing.getType(), properties, null);

    enableExplore(instance, creationProperties);
    publishAudit(instance, AuditType.UPDATE);
  }

  /**
   * Drops the specified dataset instance.
   *
   * @param instance the {@link Id.DatasetInstance} to drop
   * @throws NamespaceNotFoundException if the namespace was not found
   * @throws DatasetNotFoundException if the dataset instance was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   */
  public void drop(Id.DatasetInstance instance) throws Exception {
    // Throws NamespaceNotFoundException if the namespace does not exist
    ensureNamespaceExists(instance.getNamespace());
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new DatasetNotFoundException(instance);
    }
    LOG.info("Deleting dataset {}.{}", instance.getNamespaceId(), instance.getId());
    dropDataset(instance, spec);
    publishAudit(instance, AuditType.DELETE);
  }

  /**
   * Executes an admin operation on a dataset instance.
   *
   * @param instance the instance to execute the admin operation on
   * @param method the type of admin operation to execute
   * @return the {@link DatasetAdminOpResponse} from the HTTP handler
   * @throws NamespaceNotFoundException if the requested namespace was not found
   * @throws IOException if there was a problem in checking if the namespace exists over HTTP
   */
  public DatasetAdminOpResponse executeAdmin(Id.DatasetInstance instance, String method) throws Exception {
    // Throws NamespaceNotFoundException if the namespace does not exist
    ensureNamespaceExists(instance.getNamespace());

    Object result = null;

    // NOTE: one cannot directly call create and drop, instead this should be called thru
    //       POST/DELETE @ /data/datasets/{instance-id}. Because we must create/drop metadata for these at same time
    switch (method) {
      case "exists":
        result = opExecutorClient.exists(instance);
        break;
      case "truncate":
        opExecutorClient.truncate(instance);
        publishAudit(instance, AuditType.TRUNCATE);
        break;
      case "upgrade":
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
   * @param namespaceId {@link Id.Namespace} for the specified namespace
   * @param typeName the name of the dataset type to search
   * @return {@link DatasetTypeMeta} for the type if found in either the specified namespace or in the system namespace,
   * null otherwise.
   * TODO: This may need to move to a util class eventually
   */
  @Nullable
  private DatasetTypeMeta getTypeInfo(Id.Namespace namespaceId, String typeName) throws BadRequestException {
    Id.DatasetType datasetTypeId = ConversionHelpers.toDatasetTypeId(namespaceId, typeName);
    DatasetTypeMeta typeMeta = typeManager.getTypeInfo(datasetTypeId);
    if (typeMeta == null) {
      // Type not found in the instance's namespace. Now try finding it in the system namespace
      Id.DatasetType systemDatasetTypeId = ConversionHelpers.toDatasetTypeId(Id.Namespace.SYSTEM, typeName);
      typeMeta = typeManager.getTypeInfo(systemDatasetTypeId);
    }
    return typeMeta;
  }

  /**
   * Drops a dataset.
   *
   * @param spec specification of dataset to be dropped.
   * @throws Exception on error.
   */
  private void dropDataset(Id.DatasetInstance instance, DatasetSpecification spec) throws Exception {
    disableExplore(instance);

    if (!instanceManager.delete(instance)) {
      throw new DatasetNotFoundException(instance);
    }
    metaCache.invalidate(instance);

    DatasetTypeMeta typeMeta = getTypeInfo(instance.getNamespace(), spec.getType());
    if (typeMeta == null) {
      throw new DatasetNotFoundException(instance);
    }
    opExecutorClient.drop(instance, typeMeta, spec);
  }

  private void disableExplore(Id.DatasetInstance datasetInstance) {
    // Disable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-8
    try {
      exploreFacade.disableExploreDataset(datasetInstance);
    } catch (Exception e) {
      String msg = String.format("Cannot disable exploration of dataset instance %s: %s",
                                 datasetInstance, e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  private void enableExplore(Id.DatasetInstance datasetInstance, DatasetInstanceConfiguration creationProperties) {
    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - CDAP-8
    try {
      exploreFacade.enableExploreDataset(datasetInstance);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 datasetInstance, creationProperties.getProperties(), e.getMessage());
      LOG.error(msg, e);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  /**
   * Throws an exception if the specified namespace is not the system namespace and does not exist
   */
  private void ensureNamespaceExists(Id.Namespace namespace) throws Exception {
    if (!Id.Namespace.SYSTEM.equals(namespace)) {
      if (namespaceQueryAdmin.get(namespace) == null) {
        throw new NamespaceNotFoundException(namespace);
      }
    }
  }

  private void publishAudit(Id.DatasetInstance datasetInstance, AuditType auditType) {
    // TODO: Add properties to Audit Payload (CDAP-5220)
    AuditPublishers.publishAudit(auditPublisher, datasetInstance, auditType, AuditPayload.EMPTY_PAYLOAD);
  }
}

