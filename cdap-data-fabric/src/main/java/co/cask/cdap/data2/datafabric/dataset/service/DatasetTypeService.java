/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.module.DatasetType;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.DatasetModuleCannotBeDeletedException;
import co.cask.cdap.common.DatasetModuleNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetModuleConflictException;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.data2.security.Impersonator;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Manages lifecycle of dataset {@link DatasetType types} and {@link DatasetModule modules}.
 */
public class DatasetTypeService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeService.class);

  private final DatasetTypeManager typeManager;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final Authorizer authorizer;
  private final AuthenticationContext authenticationContext;
  private final CConfiguration cConf;
  private final Impersonator impersonator;

  @Inject
  @VisibleForTesting
  public DatasetTypeService(DatasetTypeManager typeManager, NamespaceQueryAdmin namespaceQueryAdmin,
                            NamespacedLocationFactory namespacedLocationFactory,
                            AuthorizationEnforcer authorizationEnforcer,
                            AuthorizerInstantiator authorizerInstantiator, AuthenticationContext authenticationContext,
                            CConfiguration cConf, Impersonator impersonator) {
    this.typeManager = typeManager;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.authorizationEnforcer = authorizationEnforcer;
    this.authorizer = authorizerInstantiator.get();
    this.authenticationContext = authenticationContext;
    this.cConf = cConf;
    this.impersonator = impersonator;
  }

  /**
   * Returns all {@link DatasetModuleMeta dataset modules} in the specified {@link NamespaceId namespace}.
   */
  List<DatasetModuleMeta> listModules(final NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId);
    // Sorting by name for convenience
    List<DatasetModuleMeta> allModules = Lists.newArrayList(typeManager.getModules(namespaceId.toId()));
    Collections.sort(allModules, new Comparator<DatasetModuleMeta>() {
      @Override
      public int compare(DatasetModuleMeta o1, DatasetModuleMeta o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> authFilter = authorizationEnforcer.createFilter(principal);
    Iterable<DatasetModuleMeta> authorizedDatasetModules =
      Iterables.filter(allModules, new com.google.common.base.Predicate<DatasetModuleMeta>() {
        @Override
        public boolean apply(DatasetModuleMeta datasetModuleMeta) {
          return authFilter.apply(namespaceId.datasetModule(datasetModuleMeta.getName()));
        }
      });
    return Lists.newArrayList(authorizedDatasetModules);
  }

  /**
   * Returns the {@link DatasetModuleMeta metadata} of the specified {@link DatasetModuleId}.
   */
  DatasetModuleMeta getModule(DatasetModuleId datasetModuleId) throws Exception {
    ensureNamespaceExists(datasetModuleId.getParent());
    Id.DatasetModule moduleId = datasetModuleId.toId();
    DatasetModuleMeta moduleMeta = typeManager.getModule(moduleId);
    if (moduleMeta == null) {
      throw new DatasetModuleNotFoundException(moduleId);
    }
    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!Principal.SYSTEM.equals(principal) && !filter.apply(datasetModuleId)) {
      throw new UnauthorizedException(principal, datasetModuleId);
    }
    return moduleMeta;
  }

  /**
   * Adds a new {@link DatasetModule}.
   *
   * @param datasetModuleId the {@link DatasetModuleId} for the module to be added
   * @param className the module class name specified in the HTTP header
   * @param forceUpdate if true, an update will be allowed even if there are conflicts with other modules, or if
   *                     removal of a type would break other modules' dependencies
   * @return a {@link BodyConsumer} to upload the module jar in chunks
   * @throws NotFoundException if the namespace in which the module is being added is not found
   * @throws IOException if there are issues while performing I/O like creating temporary directories, moving/unpacking
   *                      module jar files
   * @throws DatasetModuleConflictException if #forceUpdate is {@code false}, and there are conflicts with other modules
   */
  BodyConsumer addModule(final DatasetModuleId datasetModuleId, final String className,
                         final boolean forceUpdate) throws Exception {
    NamespaceId namespaceId = datasetModuleId.getParent();
    final Principal principal = authenticationContext.getPrincipal();
    // enforce that the principal has WRITE access on the namespace
    authorizationEnforcer.enforce(namespaceId, principal, Action.WRITE);
    if (NamespaceId.SYSTEM.equals(namespaceId)) {
      throw new UnauthorizedException(String.format("Cannot add module '%s' to '%s' namespace.",
                                                    datasetModuleId.getModule(), datasetModuleId.getNamespace()));
    }
    ensureNamespaceExists(namespaceId);

    // It is now determined that a new dataset module will be deployed. First grant privileges, then deploy the module.
    // If creation fails, revoke the granted privileges. This ensures that just like delete, there may be orphaned
    // privileges in rare scenarios, but there can never be orphaned datasets.
    // If the module previously existed and was deleted, but revoking privileges somehow failed, there may be orphaned
    // privileges for the module. Revoke them first, so no users unintentionally get privileges on the dataset.
    revokeAllPrivilegesOnModule(datasetModuleId);
    grantAllPrivilegesOnModule(datasetModuleId, principal);
    try {
      return createModuleConsumer(datasetModuleId, className, forceUpdate, principal);
    } catch (Exception e) {
      revokeAllPrivilegesOnModule(datasetModuleId);
      throw e;
    }
  }

  /**
   * Deletes the specified {@link DatasetModuleId}
   */
  void delete(DatasetModuleId datasetModuleId) throws Exception {
    NamespaceId namespaceId = datasetModuleId.getParent();
    if (NamespaceId.SYSTEM.equals(namespaceId)) {
      throw new UnauthorizedException(String.format("Cannot delete module '%s' from '%s' namespace.",
                                                    datasetModuleId.getModule(), datasetModuleId.getNamespace()));
    }
    ensureNamespaceExists(namespaceId);

    Id.DatasetModule module = datasetModuleId.toId();
    DatasetModuleMeta moduleMeta = typeManager.getModule(module);
    if (moduleMeta == null) {
      throw new DatasetModuleNotFoundException(module);
    }

    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(datasetModuleId, principal, Action.ADMIN);

    try {
      typeManager.deleteModule(module);
    } catch (DatasetModuleConflictException e) {
      throw new DatasetModuleCannotBeDeletedException(datasetModuleId, e.getMessage());
    }

    // revoke all privileges on the module to be deleted
    revokeAllPrivilegesOnModule(datasetModuleId, moduleMeta);
  }

  /**
   * Deletes all {@link DatasetModuleMeta dataset modules} in the specified {@link NamespaceId namespace}.
   */
  void deleteAll(NamespaceId namespaceId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(namespaceId, principal, Action.ADMIN);

    if (NamespaceId.SYSTEM.equals(namespaceId)) {
      throw new UnauthorizedException(String.format("Cannot delete modules from '%s' namespace.", namespaceId));
    }
    ensureNamespaceExists(namespaceId);

    // revoke all privileges on all modules
    Id.Namespace namespace = namespaceId.toId();
    for (DatasetModuleMeta meta : typeManager.getModules(namespace)) {
      authorizer.revoke(namespaceId.datasetModule(meta.getName()));
    }
    try {
      typeManager.deleteModules(namespace);
    } catch (DatasetModuleConflictException e) {
      throw new ConflictException(e.getMessage(), e);
    }
  }

  /**
   * Lists all {@link DatasetType dataset types} in the specified {@link NamespaceId}.
   */
  List<DatasetTypeMeta> listTypes(final NamespaceId namespaceId) throws Exception {
    ensureNamespaceExists(namespaceId);
    // Sorting by name for convenience
    List<DatasetTypeMeta> allTypes = Lists.newArrayList(typeManager.getTypes(namespaceId.toId()));
    Collections.sort(allTypes, new Comparator<DatasetTypeMeta>() {
      @Override
      public int compare(DatasetTypeMeta o1, DatasetTypeMeta o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    Principal principal = authenticationContext.getPrincipal();
    final Predicate<EntityId> authFilter = authorizationEnforcer.createFilter(principal);
    Iterable<DatasetTypeMeta> authorizedDatasetTypes =
      Iterables.filter(allTypes, new com.google.common.base.Predicate<DatasetTypeMeta>() {
        @Override
        public boolean apply(DatasetTypeMeta datasetTypeMeta) {
          DatasetTypeId datasetTypeId = namespaceId.datasetType(datasetTypeMeta.getName());
          return authFilter.apply(datasetTypeId);
        }
      });
    return Lists.newArrayList(authorizedDatasetTypes);
  }

  /**
   * Returns details of the specified {@link DatasetTypeId dataset type}.
   */
  DatasetTypeMeta getType(DatasetTypeId datasetTypeId) throws Exception {
    ensureNamespaceExists(datasetTypeId.getParent());
    Id.DatasetType datasetType = datasetTypeId.toId();
    DatasetTypeMeta typeMeta = typeManager.getTypeInfo(datasetType);
    if (typeMeta == null) {
      throw new DatasetTypeNotFoundException(datasetType);
    }

    // only return the type if the user has some privileges on it
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> authFilter = authorizationEnforcer.createFilter(principal);
    if (!Principal.SYSTEM.equals(principal) && !authFilter.apply(datasetTypeId)) {
      throw new UnauthorizedException(principal, datasetTypeId);
    }
    return typeMeta;
  }

  private AbstractBodyConsumer createModuleConsumer(final DatasetModuleId datasetModuleId,
                                                    final String className, final boolean forceUpdate,
                                                    final Principal principal) throws IOException, NotFoundException {
    final NamespaceId namespaceId = datasetModuleId.getParent();
    final Location namespaceHomeLocation;
    try {
      namespaceHomeLocation = impersonator.doAs(namespaceId, new Callable<Location>() {
        @Override
        public Location call() throws Exception {
          return namespacedLocationFactory.get(namespaceId.toId());
        }
      });
    } catch (Exception e) {
      // the only checked exception that the callable throws is IOException
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }

    // verify namespace directory exists
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found", namespaceHomeLocation, namespaceId);
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
          responder.sendString(HttpResponseStatus.BAD_REQUEST, "Required header 'class-name' is absent.");
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
            throw new IOException(String.format("Could not move archive from location: %s, to location: %s",
                                                tmpLocation, archive));
          }

          typeManager.addModule(datasetModuleId.toId(), className, archive, forceUpdate);
          // todo: response with DatasetModuleMeta of just added module (and log this info)
          // Ideally this should have been done before, but we cannot grant privileges on types until they've been
          // added to the type MDS. First revoke any orphaned privileges for types left behind by past failed revokes
          revokeAllPrivilegesOnModule(datasetModuleId);
          grantAllPrivilegesOnModule(datasetModuleId, principal);
          LOG.info("Added module {}", datasetModuleId);
          responder.sendStatus(HttpResponseStatus.OK);
        } catch (Exception e) {
          // There was a problem in deploying the dataset module. so revoke the privileges.
          revokeAllPrivilegesOnModule(datasetModuleId);
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

  private void grantAllPrivilegesOnModule(DatasetModuleId moduleId, Principal principal) throws Exception {
    Set<Action> allActions = ImmutableSet.of(Action.ALL);
    authorizer.grant(moduleId, principal, allActions);
    DatasetModuleMeta moduleMeta = typeManager.getModule(moduleId.toId());
    if (moduleMeta == null) {
      LOG.debug("Could not find metadata for module {}. Not granting privileges for its types.", moduleId);
      return;
    }
    for (String type : moduleMeta.getTypes()) {
      DatasetTypeId datasetTypeId = moduleId.getParent().datasetType(type);
      authorizer.grant(datasetTypeId, principal, allActions);
    }
  }

  private void revokeAllPrivilegesOnModule(DatasetModuleId moduleId) throws Exception {
    revokeAllPrivilegesOnModule(moduleId, null);
  }

  private void revokeAllPrivilegesOnModule(DatasetModuleId moduleId,
                                           @Nullable DatasetModuleMeta moduleMeta) throws Exception {
    authorizer.revoke(moduleId);
    moduleMeta = moduleMeta == null ? typeManager.getModule(moduleId.toId()) : moduleMeta;
    if (moduleMeta == null) {
      LOG.debug("Could not find metadata for module {}. Will not revoke privileges for its types.", moduleId);
      return;
    }
    for (String type : moduleMeta.getTypes()) {
      DatasetTypeId datasetTypeId = moduleId.getParent().datasetType(type);
      authorizer.revoke(datasetTypeId);
    }
  }

  /**
   * Throws an exception if the specified namespace is not the system namespace and does not exist
   */
  private void ensureNamespaceExists(NamespaceId namespaceId) throws Exception {
    if (!NamespaceId.SYSTEM.equals(namespaceId)) {
      Id.Namespace namespace = namespaceId.toId();
      if (namespaceQueryAdmin.get(namespace) == null) {
        throw new NamespaceNotFoundException(namespace);
      }
    }
  }
}
