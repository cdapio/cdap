/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.DatasetAlreadyExistsException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.HandlerException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.AbstractDatasetProvider;
import co.cask.cdap.data2.datafabric.dataset.instance.DatasetInstanceManager;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpResponse;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Handles dataset instance management calls.
 */
public class DatasetInstanceService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceService.class);

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;
  private final DatasetOpExecutor opExecutorClient;
  private final ExploreFacade exploreFacade;
  private final boolean allowDatasetUncheckedUpgrade;
  private final UsageRegistry usageRegistry;

  @Inject
  public DatasetInstanceService(DatasetTypeManager implManager, DatasetInstanceManager instanceManager,
                                DatasetOpExecutor opExecutorClient, ExploreFacade exploreFacade, CConfiguration conf,
                                TransactionExecutorFactory txFactory,
                                DatasetDefinitionRegistryFactory registryFactory) {
    this.opExecutorClient = opExecutorClient;
    this.implManager = implManager;
    this.instanceManager = instanceManager;
    this.exploreFacade = exploreFacade;
    this.usageRegistry = new UsageRegistry(txFactory, new AbstractDatasetProvider(registryFactory) {
      @Override
      public DatasetMeta getMeta(Id.DatasetInstance instance) throws Exception {
        return DatasetInstanceService.this.get(instance, ImmutableList.<Id>of());
      }

      @Override
      public void createIfNotExists(Id.DatasetInstance instance, String type,
                                    DatasetProperties creationProps) throws Exception {
        DatasetInstanceService.this.createIfNotExists(
          instance.getNamespace(), instance.getId(),
          new DatasetInstanceConfiguration(type, creationProps.getProperties()));
      }
    });
    this.allowDatasetUncheckedUpgrade = conf.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
  }

  /**
   * Lists all dataset instances in a namespace.
   *
   * @param namespace the namespace to list datasets for
   * @return the dataset instances in the provided namespace
   */
  public Collection<DatasetSpecification> list(Id.Namespace namespace) {
    return instanceManager.getAll(namespace);
  }

  /**
   * Gets a dataset instance.
   *
   * @param instance instance to get
   * @param owners the {@link Id}s that will be using the dataset instance
   * @return the dataset instance's {@link DatasetMeta}
   * @throws NotFoundException if the dataset instance is not found
   */
  public DatasetMeta get(Id.DatasetInstance instance, List<? extends Id> owners) throws NotFoundException {
    // TODO: CDAP-3901 add back namespace existence check
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new NotFoundException(instance);
    }

    Id.DatasetType datasetTypeId = Id.DatasetType.from(instance.getNamespace(), spec.getType());
    DatasetTypeMeta typeMeta = getTypeInfo(instance.getNamespace(), spec.getType());
    if (typeMeta == null) {
      // TODO: This shouldn't happen unless CDAP is in an invalid state - maybe give different error
      throw new NotFoundException(datasetTypeId);
    }

    registerUsage(instance, owners);
    return new DatasetMeta(spec, typeMeta, null);
  }

  private void registerUsage(Id.DatasetInstance instance, List<? extends Id> owners) {
    for (Id owner : owners) {
      try {
        if (owner instanceof Id.Program) {
          usageRegistry.register((Id.Program) owner, instance);
        } else if (owner instanceof Id.Adapter) {
          usageRegistry.register((Id.Adapter) owner, instance);
        }
      } catch (Exception e) {
        LOG.warn("Failed to register usage of {} -> {}", owner, instance);
      }
    }
  }

  /**
   * Creates a dataset instance.
   *
   * @param namespace the namespace to create the dataset instance in
   * @param name the name of the new dataset instance
   * @param props the properties for the new dataset instance
   * @throws DatasetAlreadyExistsException if a dataset with the same name already exists
   * @throws DatasetTypeNotFoundException if the dataset type was not found
   * @throws Exception if something went wrong
   */
  public void create(Id.Namespace namespace, String name, DatasetInstanceConfiguration props) throws Exception {
    Id.DatasetInstance newInstance = Id.DatasetInstance.from(namespace, name);
    DatasetSpecification existing = instanceManager.get(newInstance);
    if (existing != null && !allowDatasetUncheckedUpgrade) {
      throw new DatasetAlreadyExistsException(newInstance);
    }

    // Disable explore if the table already existed
    if (existing != null) {
      disableExplore(newInstance);
    }

    DatasetTypeMeta typeMeta = getTypeInfo(namespace, props.getTypeName());
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(Id.DatasetType.from(namespace, props.getTypeName()));
    }

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec = opExecutorClient.create(newInstance, typeMeta,
                                                        DatasetProperties.builder()
                                                          .addAll(props.getProperties())
                                                          .build());
    instanceManager.add(namespace, spec);

    // Enable explore
    enableExplore(newInstance, props);
  }

  private void createIfNotExists(Id.Namespace namespace, String name,
                                 DatasetInstanceConfiguration props) throws Exception {
    try {
      create(namespace, name, props);
    } catch (DatasetAlreadyExistsException e) {
      // ignore
    }
  }

  /**
   * Updates an existing Dataset specification properties.
   * {@link DatasetInstanceConfiguration} is constructed based on request and the Dataset instance is updated.
   *
   * @param instance the dataset instance
   * @param properties the dataset properties to be used
   * @throws DatasetNotFoundException if the dataset was not found
   * @throws DatasetTypeNotFoundException if the type of the existing dataset was not found
   */
  public void update(Id.DatasetInstance instance, Map<String, String> properties) throws Exception {
    DatasetSpecification existing = instanceManager.get(instance);
    if (existing == null) {
      throw new DatasetNotFoundException(instance);
    }

    disableExplore(instance);

    DatasetTypeMeta typeMeta = getTypeInfo(instance.getNamespace(), existing.getType());
    if (typeMeta == null) {
      // Type not found in the instance's namespace and the system namespace. Bail out.
      throw new DatasetTypeNotFoundException(Id.DatasetType.from(instance.getNamespace(), existing.getType()));
    }

    // Note how we execute configure() via opExecutorClient (outside of ds service) to isolate running user code
    DatasetSpecification spec = opExecutorClient.create(instance, typeMeta,
                                                        DatasetProperties.builder()
                                                          .addAll(properties)
                                                          .build());
    instanceManager.add(instance.getNamespace(), spec);

    DatasetInstanceConfiguration creationProperties = new DatasetInstanceConfiguration(existing.getType(), properties);
    enableExplore(instance, creationProperties);

    //caling admin upgrade, after updating specification
    executeAdmin(instance, "upgrade");
  }

  public void drop(Id.DatasetInstance instance) throws Exception {
    LOG.info("Deleting dataset {}.{}", instance.getNamespaceId(), instance.getId());
    DatasetSpecification spec = instanceManager.get(instance);
    if (spec == null) {
      throw new DatasetNotFoundException(instance);
    }

    dropDataset(instance, spec);
  }

  /**
   * Executes an admin operation on a dataset instance.
   *
   * @param instance the instance to execute the admin operation on
   * @param method the type of admin operation to execute
   * @return the {@link DatasetAdminOpResponse} from the HTTP handler
   * @throws Exception if something went wrong
   */
  public DatasetAdminOpResponse executeAdmin(Id.DatasetInstance instance, String method) throws Exception {
    Object result = null;

    // NOTE: one cannot directly call create and drop, instead this should be called thru
    //       POST/DELETE @ /data/datasets/{instance-id}. Because we must create/drop metadata for these at same time
    if (method.equals("exists")) {
      result = opExecutorClient.exists(instance);
    } else if (method.equals("truncate")) {
      opExecutorClient.truncate(instance);
    } else if (method.equals("upgrade")) {
      opExecutorClient.upgrade(instance);
    } else {
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
  private DatasetTypeMeta getTypeInfo(Id.Namespace namespaceId, String typeName) {
    Id.DatasetType datasetTypeId = Id.DatasetType.from(namespaceId, typeName);
    DatasetTypeMeta typeMeta = implManager.getTypeInfo(datasetTypeId);
    if (typeMeta == null) {
      // Type not found in the instance's namespace. Now try finding it in the system namespace
      Id.DatasetType systemDatasetTypeId = Id.DatasetType.from(Constants.SYSTEM_NAMESPACE_ID, typeName);
      typeMeta = implManager.getTypeInfo(systemDatasetTypeId);
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
}
