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
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.dataset.module.DatasetType;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.DatasetModuleCannotBeDeletedException;
import io.cdap.cdap.common.DatasetModuleNotFoundException;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data2.datafabric.dataset.service.mds.DatasetTypeTable;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetModuleConflictException;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.twill.filesystem.Location;
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
  private final NamespacePathLocator namespacePathLocator;
  private final TransactionRunner transactionRunner;

  private final CConfiguration cConf;
  private final Impersonator impersonator;
  private final TransactionSystemClientService txClientService;
  private final Map<String, DatasetModule> defaultModules;
  private final Map<String, DatasetModule> extensionModules;

  @Inject
  @VisibleForTesting
  public DefaultDatasetTypeService(DatasetTypeManager typeManager,
      NamespaceQueryAdmin namespaceQueryAdmin,
      NamespacePathLocator namespacePathLocator,
      CConfiguration cConf, Impersonator impersonator,
      TransactionSystemClientService txClientService,
      TransactionRunner transactionRunner,
      @Constants.Dataset.Manager.DefaultDatasetModules
          Map<String, DatasetModule> modules) {
    this.typeManager = typeManager;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.namespacePathLocator = namespacePathLocator;
    this.cConf = cConf;
    this.impersonator = impersonator;
    this.txClientService = txClientService;
    this.defaultModules = new LinkedHashMap<>(modules);
    this.extensionModules = getExtensionModules(cConf);
    this.transactionRunner = transactionRunner;
  }

  @Override
  protected void startUp() throws Exception {
    txClientService.startAndWait();
    deleteSystemModules();
    deployDefaultModules();
    if (!extensionModules.isEmpty()) {
      deployExtensionModules();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    txClientService.stopAndWait();
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
   * Adds a new {@link DatasetModule}.
   *
   * @param datasetModuleId the {@link DatasetModuleId} for the module to be added
   * @param className the module class name specified in the HTTP header
   * @param forceUpdate if true, an update will be allowed even if there are conflicts with
   *     other modules, or if removal of a type would break other modules' dependencies
   * @return a {@link BodyConsumer} to upload the module jar in chunks
   * @throws NotFoundException if the namespace in which the module is being added is not found
   * @throws IOException if there are issues while performing I/O like creating temporary
   *     directories, moving/unpacking module jar files
   * @throws DatasetModuleConflictException if #forceUpdate is {@code false}, and there are
   *     conflicts with other modules
   */
  @Override
  public BodyConsumer addModule(final DatasetModuleId datasetModuleId, final String className,
      final boolean forceUpdate) throws Exception {
    NamespaceId namespaceId = datasetModuleId.getParent();
    ensureNamespaceExists(namespaceId);

    // It is now determined that a new dataset module will be deployed. First grant privileges, then deploy the module.
    // If creation fails, revoke the granted privileges. This ensures that just like delete, there may be orphaned
    // privileges in rare scenarios, but there can never be orphaned datasets.
    // If the module previously existed and was deleted, but revoking privileges somehow failed, there may be orphaned
    // privileges for the module. Revoke them first, so no users unintentionally get privileges on the dataset.
    return createModuleConsumer(datasetModuleId, className, forceUpdate);
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

  private AbstractBodyConsumer createModuleConsumer(final DatasetModuleId datasetModuleId,
      final String className,
      final boolean forceUpdate) throws IOException, NotFoundException {
    final NamespaceId namespaceId = datasetModuleId.getParent();
    final Location namespaceHomeLocation;
    try {
      namespaceHomeLocation = impersonator.doAs(namespaceId, new Callable<Location>() {
        @Override
        public Location call() throws Exception {
          return namespacePathLocator.get(namespaceId);
        }
      });
    } catch (Exception e) {
      // the only checked exception that the callable throws is IOException
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }

    // verify namespace directory exists
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
          namespaceHomeLocation, namespaceId);
      LOG.debug(msg);
      throw new NotFoundException(msg);
    }

    // Store uploaded content to a local temp file
    String namespacesDir = cConf.get(Constants.Namespace.NAMESPACES_DIR);
    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    File namespaceBase = new File(localDataDir, namespacesDir);
    File tempDir = new File(new File(namespaceBase, datasetModuleId.getNamespace()),
        cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    return new AbstractBodyConsumer(File.createTempFile("dataset-", ".jar", tempDir)) {
      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) throws Exception {
        if (className == null) {
          // We have to delay until body upload is completed due to the fact that not all client is
          // requesting with "Expect: 100-continue" header and the client library we have cannot handle
          // connection close, and yet be able to read response reliably.
          // In longer term we should fix the client, as well as the netty-http server. However, since
          // this handler will be gone in near future, it's ok to have this workaround.
          responder.sendString(HttpResponseStatus.BAD_REQUEST,
              "Required header 'class-name' is absent.");
          return;
        }

        LOG.debug("Adding module {}, class name: {}", datasetModuleId, className);

        String dataFabricDir = cConf.get(Constants.Dataset.Manager.OUTPUT_DIR);
        String moduleName = datasetModuleId.getModule();
        Location archiveDir = namespaceHomeLocation.append(dataFabricDir).append(moduleName)
            .append(Constants.ARCHIVE_DIR);
        String archiveName = moduleName + ".jar";
        Location archive = archiveDir.append(archiveName);

        // Copy uploaded content to a temporary location
        Location tmpLocation = archive.getTempFile(".tmp");
        try {
          Locations.mkdirsIfNotExists(archiveDir);

          LOG.debug("Copy from {} to {}", uploadedFile, tmpLocation);
          Files.copy(uploadedFile, Locations.newOutputSupplier(tmpLocation));

          // Finally, move archive to final location
          LOG.debug("Storing module {} jar at {}", datasetModuleId, archive);
          if (tmpLocation.renameTo(archive) == null) {
            throw new IOException(
                String.format("Could not move archive from location: %s, to location: %s",
                    tmpLocation, archive));
          }

          typeManager.addModule(datasetModuleId, className, archive, forceUpdate);
          // todo: response with DatasetModuleMeta of just added module (and log this info)
          // Ideally this should have been done before, but we cannot grant privileges on types until they've been
          // added to the type MDS. First revoke any orphaned privileges for types left behind by past failed revokes
          LOG.info("Added module {}", datasetModuleId);
          responder.sendStatus(HttpResponseStatus.OK);
        } catch (Exception e) {
          // There was a problem in deploying the dataset module. so revoke the privileges.
          // In case copy to temporary file failed, or rename failed
          try {
            tmpLocation.delete();
          } catch (IOException ex) {
            LOG.warn("Failed to cleanup temporary location {}", tmpLocation);
          }
          if (e instanceof DatasetModuleConflictException) {
            responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
          } else {
            throw e;
          }
        }
      }
    };
  }

  private void deleteSystemModules() {
    TransactionRunners.run(transactionRunner, context -> {
      DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
      Collection<DatasetModuleMeta> allDatasets = datasetTypeTable.getModules(NamespaceId.SYSTEM);
      for (DatasetModuleMeta ds : allDatasets) {
        if (ds.getJarLocationPath() == null) {
          LOG.debug("Deleting system dataset module: {}", ds.toString());
          DatasetModuleId moduleId = NamespaceId.SYSTEM.datasetModule(ds.getName());
          datasetTypeTable.deleteModule(moduleId);
        }
      }
    });
  }

  private void deployDefaultModules() throws Exception {
    // adding default modules to be available in dataset manager service
    for (Map.Entry<String, DatasetModule> module : defaultModules.entrySet()) {
      try {
        // NOTE: we assume default modules are always in classpath, hence passing null for jar location
        // NOTE: we add default modules in the system namespace
        DatasetModuleId defaultModule = NamespaceId.SYSTEM.datasetModule(module.getKey());
        typeManager.addModule(defaultModule, module.getValue().getClass().getName(), null, false);
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
        // NOTE: we assume extension modules are always in classpath, hence passing null for jar location
        // NOTE: we add extension modules in the system namespace
        DatasetModuleId theModule = NamespaceId.SYSTEM.datasetModule(module.getKey());
        typeManager.addModule(theModule, module.getValue().getClass().getName(), null, false);
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
