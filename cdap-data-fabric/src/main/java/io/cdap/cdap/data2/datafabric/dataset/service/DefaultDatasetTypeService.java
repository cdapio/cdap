/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.module.DatasetType;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.DatasetModuleCannotBeDeletedException;
import io.cdap.cdap.common.DatasetModuleNotFoundException;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.data2.datafabric.dataset.service.mds.DatasetTypeTable;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetModuleConflictException;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link DatasetTypeService}, all the operation does not have
 * authorization enforce involved
 */
public class DefaultDatasetTypeService extends AbstractIdleService implements DatasetTypeService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDatasetTypeService.class);

  private final DatasetTypeManager typeManager;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final TransactionRunner transactionRunner;
  private final TransactionSystemClientService txClientService;
  private final Map<String, DatasetModule> defaultModules;
  private final Map<String, DatasetModule> extensionModules;

  @Inject
  @VisibleForTesting
  public DefaultDatasetTypeService(DatasetTypeManager typeManager,
      NamespaceQueryAdmin namespaceQueryAdmin,
      CConfiguration cConf,
      TransactionSystemClientService txClientService,
      TransactionRunner transactionRunner,
      @Constants.Dataset.Manager.DefaultDatasetModules
          Map<String, DatasetModule> modules) {
    this.typeManager = typeManager;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.txClientService = txClientService;
    this.defaultModules = new LinkedHashMap<>(modules);
    this.extensionModules = getExtensionModules(cConf);
    this.transactionRunner = transactionRunner;
  }

  @Override
  protected void startUp() throws Exception {
    Services.startAndWait(txClientService);
    deleteSystemModules();
    deployDefaultModules();
    if (!extensionModules.isEmpty()) {
      deployExtensionModules();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    Services.stopAndWait(txClientService);
  }

  /**
   * Returns all {@link DatasetModuleMeta dataset modules} in the specified {@link NamespaceId
   * namespace}.
   */
  @Override
  public List<DatasetModuleMeta> listModules(final NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId);
    // Sorting by name for convenience
    List<DatasetModuleMeta> allModules = Lists.newArrayList(typeManager.getModules(namespaceId));
    Collections.sort(allModules, new Comparator<DatasetModuleMeta>() {
      @Override
      public int compare(DatasetModuleMeta o1, DatasetModuleMeta o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    return allModules;
  }

  /**
   * Returns the {@link DatasetModuleMeta metadata} of the specified {@link DatasetModuleId}.
   */
  @Override
  public DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws Exception {
    ensureNamespaceExists(datasetModuleId.getParent());
    DatasetModuleMeta moduleMeta = typeManager.getModule(datasetModuleId);

    if (moduleMeta == null) {
      throw new DatasetModuleNotFoundException(datasetModuleId);
    }
    return moduleMeta;
  }

  /**
   * Deletes the specified {@link DatasetModuleId}
   */
  @Override
  public void delete(DatasetModuleId datasetModuleId) throws Exception {
    NamespaceId namespaceId = datasetModuleId.getParent();
    if (NamespaceId.SYSTEM.equals(namespaceId)) {
      throw new UnsupportedOperationException(
          String.format("Cannot delete module '%s' from '%s' namespace.",
              datasetModuleId.getModule(), datasetModuleId.getNamespace()));
    }
    ensureNamespaceExists(namespaceId);

    DatasetModuleMeta moduleMeta = typeManager.getModule(datasetModuleId);
    if (moduleMeta == null) {
      throw new DatasetModuleNotFoundException(datasetModuleId);
    }

    try {
      typeManager.deleteModule(datasetModuleId);
    } catch (DatasetModuleConflictException e) {
      throw new DatasetModuleCannotBeDeletedException(datasetModuleId, e.getMessage());
    }
  }

  /**
   * Deletes all {@link DatasetModuleMeta dataset modules} in the specified {@link NamespaceId
   * namespace}.
   */
  @Override
  public void deleteAll(NamespaceId namespaceId) throws Exception {
    if (NamespaceId.SYSTEM.equals(namespaceId)) {
      throw new UnsupportedOperationException(
          String.format("Cannot delete modules from '%s' namespace.", namespaceId));
    }
    ensureNamespaceExists(namespaceId);

    try {
      typeManager.deleteModules(namespaceId);
    } catch (DatasetModuleConflictException e) {
      throw new ConflictException(e.getMessage(), e);
    }
  }

  /**
   * Lists all {@link DatasetType dataset types} in the specified {@link NamespaceId}.
   */
  @Override
  public List<DatasetTypeMeta> listTypes(final NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId);
    // Sorting by name for convenience
    List<DatasetTypeMeta> allTypes = Lists.newArrayList(typeManager.getTypes(namespaceId));
    Collections.sort(allTypes, new Comparator<DatasetTypeMeta>() {
      @Override
      public int compare(DatasetTypeMeta o1, DatasetTypeMeta o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    return allTypes;
  }

  /**
   * Returns details of the specified {@link DatasetTypeId dataset type}.
   */
  @Override
  public DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws Exception {
    ensureNamespaceExists(datasetTypeId.getParent());
    DatasetTypeMeta typeMeta = typeManager.getTypeInfo(datasetTypeId);

    if (typeMeta == null) {
      throw new DatasetTypeNotFoundException(datasetTypeId);
    }
    return typeMeta;
  }

  @VisibleForTesting
  void deleteSystemModules() {
    TransactionRunners.run(transactionRunner, context -> {
      DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
      Collection<DatasetModuleMeta> allDatasets = datasetTypeTable.getModules(NamespaceId.SYSTEM);
      // Every module in the system namespace is registered from the classpath on startup, so all
      // existing entries are removed before the default and extension modules are added again.
      for (DatasetModuleMeta ds : allDatasets) {
        LOG.debug("Deleting system dataset module: {}", ds.toString());
        DatasetModuleId moduleId = NamespaceId.SYSTEM.datasetModule(ds.getName());
        datasetTypeTable.deleteModule(moduleId);
      }
    });
  }

  private void deployDefaultModules() throws Exception {
    // adding default modules to be available in dataset manager service
    for (Map.Entry<String, DatasetModule> module : defaultModules.entrySet()) {
      try {
        // NOTE: default modules are always in classpath
        // NOTE: we add default modules in the system namespace
        DatasetModuleId defaultModule = NamespaceId.SYSTEM.datasetModule(module.getKey());
        typeManager.addModule(defaultModule, module.getValue().getClass().getName(), false);
      } catch (DatasetModuleConflictException e) {
        // perfectly fine: we need to add default modules only the very first time service is started
        LOG.debug("Not adding {} module: it already exists", module.getKey());
      } catch (Throwable th) {
        LOG.error("Failed to add {} module. Aborting.", module.getKey(), th);
        throw Throwables.propagate(th);
      }
    }
  }

  private Map<String, DatasetModule> getExtensionModules(CConfiguration cConf) {
    Map<String, DatasetModule> modules = new LinkedHashMap<>();
    String moduleStr = cConf.get(Constants.Dataset.Extensions.MODULES);
    if (moduleStr != null) {
      for (String moduleName : Splitter.on(',').omitEmptyStrings().split(moduleStr)) {
        // create DatasetModule object
        try {
          Class tableModuleClass = Class.forName(moduleName);
          DatasetModule module = (DatasetModule) tableModuleClass.newInstance();
          modules.put(moduleName, module);
        } catch (ClassCastException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
          LOG.error("Failed to add {} extension module: {}", moduleName, ex.toString());
        }
      }
    }
    return modules;
  }

  private void deployExtensionModules() {
    // adding any defined extension modules to be available in dataset manager service
    for (Map.Entry<String, DatasetModule> module : extensionModules.entrySet()) {
      try {
        // NOTE: extension modules are always in classpath
        // NOTE: we add extension modules in the system namespace
        DatasetModuleId theModule = NamespaceId.SYSTEM.datasetModule(module.getKey());
        typeManager.addModule(theModule, module.getValue().getClass().getName(), false);
      } catch (DatasetModuleConflictException e) {
        // perfectly fine: we need to add the modules only the very first time service is started
        LOG.debug("Not adding {} extension module: it already exists", module.getKey());
      } catch (Throwable th) {
        LOG.error("Failed to add {} extension module. Aborting.", module.getKey(), th);
        throw Throwables.propagate(th);
      }
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
}
