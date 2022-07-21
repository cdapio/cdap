/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.type;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.DirectoryClassLoader;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
import io.cdap.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceTable;
import io.cdap.cdap.data2.datafabric.dataset.service.mds.DatasetTypeTable;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistries;
import io.cdap.cdap.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import io.cdap.cdap.data2.dataset2.TypeConflictException;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Manages dataset types and modules metadata
 */
@VisibleForTesting
public class DatasetTypeManager {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeManager.class);

  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final Path systemTempPath;
  private final Impersonator impersonator;
  private final TransactionRunner transactionRunner;

  @VisibleForTesting
  @Inject
  public DatasetTypeManager(CConfiguration cConf,
                            LocationFactory locationFactory,
                            Impersonator impersonator,
                            TransactionRunner transactionRunner) {
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.impersonator = impersonator;

    this.systemTempPath = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                    cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath();
    this.transactionRunner = transactionRunner;
  }

  /**
   * Add datasets module in a namespace
   *
   * @param datasetModuleId the {@link DatasetModuleId} to add
   * @param className module class
   * @param jarLocation location of the module jar
   * @param force if true, an update will be allowed even if there are conflicts with other modules, or if
   *                     removal of a type would break other modules' dependencies.
   */
  public void addModule(final DatasetModuleId datasetModuleId, final String className, final Location jarLocation,
                        final boolean force)
    throws DatasetModuleConflictException {

    LOG.debug("adding module: {}, className: {}, jarLocation: {}",
              datasetModuleId, className, jarLocation == null ? "[local]" : jarLocation);

    try {
      TransactionRunners.run(transactionRunner, context -> {
      final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
      final DatasetInstanceTable datasetInstanceTable = new DatasetInstanceTable(context);
        // 1. get existing module with all its types
        DatasetModuleMeta existing = datasetTypeTable.getModule(datasetModuleId);
        DependencyTrackingRegistry reg;

        // 2. unpack jar and create class loader
        ClassLoaderFolder classLoaderFolder = null;
        DirectoryClassLoader cl = null;
        try {
          // NOTE: if jarLocation is null, we assume that this is a system module, ie. always present in classpath
          if (jarLocation != null) {
            classLoaderFolder = BundleJarUtil.prepareClassLoaderFolder(
              jarLocation, () -> Files.createTempDirectory(Files.createDirectories(systemTempPath),
                                                           datasetModuleId.getEntityName()).toFile());
            cl = new DirectoryClassLoader(classLoaderFolder.getDir(),
                                          cConf.get(Constants.AppFabric.PROGRAM_EXTRA_CLASSPATH),
                                          FilterClassLoader.create(getClass().getClassLoader()), "lib");
          }
          reg = new DependencyTrackingRegistry(datasetModuleId, datasetTypeTable, cl, force);

          // 3. register the new module while tracking dependencies.
          //    this will fail if a type exists in a different module
          DatasetDefinitionRegistries.register(className, cl, reg);

        } catch (TypeConflictException e) {
          throw e; // type conflict from the registry, we want to throw that as is
        } catch (Exception e) {
          LOG.error("Could not instantiate instance of dataset module class {} for module {} using jarLocation {}",
                    className, datasetModuleId, jarLocation);
          throw Throwables.propagate(e);
        } finally {
          // Close the ProgramClassLoader
          Closeables.closeQuietly(cl);
          Closeables.closeQuietly(classLoaderFolder);
        }

        // 4. determine whether any type were removed from the module, and whether any other modules depend on them
        if (existing != null) {
          Set<String> removedTypes = new HashSet<>(existing.getTypes());
          removedTypes.removeAll(reg.getTypes());
          // TODO (CDAP-6294): track dependencies at the type level
          if (!force && !removedTypes.isEmpty() && !existing.getUsedByModules().isEmpty()) {
            throw new DatasetModuleConflictException(String.format(
              "Cannot update module '%s' to remove types %s: Modules %s may depend on it. Delete them first",
              datasetModuleId, removedTypes, existing.getUsedByModules()));
          }
          Collection<DatasetSpecification> instances =
            datasetInstanceTable.getByTypes(datasetModuleId.getParent(), removedTypes);
          if (!instances.isEmpty()) {
            throw new DatasetModuleConflictException(String.format(
              "Attempt to remove dataset types %s from module '%s' that have existing instances: %s. " +
                "Delete them first.", removedTypes, datasetModuleId,
              instances.stream()
                .map(input -> input.getName() + ":" + input.getType())
                .collect(Collectors.joining(", "))));
          }
        }

        // NOTE: we use set to avoid duplicated dependencies
        // NOTE: we use LinkedHashSet to preserve order in which dependencies must be loaded
        Set<String> moduleDependencies = new LinkedHashSet<String>();
        for (DatasetTypeId usedType : reg.getUsedTypes()) {
          DatasetModuleMeta usedModule = datasetTypeTable.getModuleByType(usedType);
          if (usedModule == null) {
            throw new IllegalStateException(
              String.format("Found a null used module for type %s for while adding module %s",
                            usedType, datasetModuleId));
          }
          // adding all used types and the module itself, in this very order to keep the order of loading modules
          // for instantiating a type
          moduleDependencies.addAll(usedModule.getUsesModules());
          boolean added = moduleDependencies.add(usedModule.getName());
          if (added) {
            // also adding this module as a dependent for all modules it uses
            usedModule.addUsedByModule(datasetModuleId.getEntityName());
            datasetTypeTable.writeModule(usedType.getParent(), usedModule);
          }
        }

        URI jarURI = jarLocation == null ? null : jarLocation.toURI();
        DatasetModuleMeta moduleMeta = existing == null
          ? new DatasetModuleMeta(datasetModuleId.getEntityName(), className, jarURI, reg.getTypes(),
                                  Lists.newArrayList(moduleDependencies))
          : new DatasetModuleMeta(datasetModuleId.getEntityName(), className, jarURI, reg.getTypes(),
                                  Lists.newArrayList(moduleDependencies),
                                  Lists.newArrayList(existing.getUsedByModules()));
        datasetTypeTable.writeModule(datasetModuleId.getParent(), moduleMeta);
      });
    } catch (RuntimeException e) {
      for (Throwable cause : Throwables.getCausalChain(e)) {
        if (cause instanceof DatasetModuleConflictException) {
          throw (DatasetModuleConflictException) cause;
        } else if (cause instanceof TypeConflictException) {
          throw new DatasetModuleConflictException(cause.getMessage(), cause);
        }
      }
      throw Throwables.propagate(e);
    } catch (Exception e) {
      LOG.error("Operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   *
   * @param namespaceId the {@link NamespaceId} to retrieve types from
   * @return collection of types available in the specified namespace
   */
  public Collection<DatasetTypeMeta> getTypes(final NamespaceId namespaceId) {
    return TransactionRunners.run(transactionRunner, context -> {
      final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
      return datasetTypeTable.getTypes(namespaceId);
    });
  }

  /**
   * Get dataset type information
   * @param datasetTypeId name of the type to get info for
   * @return instance of {@link DatasetTypeMeta} or {@code null} if type
   *         does NOT exist
   */
  @Nullable
  public DatasetTypeMeta getTypeInfo(final DatasetTypeId datasetTypeId) {
    return TransactionRunners.run(transactionRunner, context -> {
      final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
      return datasetTypeTable.getType(datasetTypeId);
    });
  }

  /**
   * @param namespaceId {@link NamespaceId} to retrieve the module list from
   * @return list of dataset modules information from the specified namespace
   */
  public Collection<DatasetModuleMeta> getModules(final NamespaceId namespaceId) {
    return TransactionRunners.run(transactionRunner, context -> {
      final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
          return datasetTypeTable.getModules(namespaceId);
      });
  }

  /**
   * @param datasetModuleId {@link DatasetModuleId} of the module to return info for
   * @return dataset module info or {@code null} if module with given name does NOT exist
   */
  @Nullable
  public DatasetModuleMeta getModule(final DatasetModuleId datasetModuleId) {
    return TransactionRunners.run(transactionRunner, context -> {
      final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
        return datasetTypeTable.getModule(datasetModuleId);
    });
  }

  /**
   * Deletes specified dataset module
   * @param datasetModuleId {@link DatasetModuleId} of the dataset module to delete
   * @return true if deleted successfully, false if module didn't exist: nothing to delete
   * @throws DatasetModuleConflictException when there are other modules depend on the specified one, in which case
   *         deletion does NOT happen
   */
  public boolean deleteModule(final DatasetModuleId datasetModuleId) throws DatasetModuleConflictException {
    LOG.info("Deleting module {}", datasetModuleId);
    try {
      return TransactionRunners.run(transactionRunner, context -> {
        final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
        final DatasetInstanceTable datasetInstanceTable = new DatasetInstanceTable(context);
        final DatasetModuleMeta module = datasetTypeTable.getModule(datasetModuleId);

        if (module == null) {
          return false;
        }

        // cannot delete when there's module that uses it
        if (module.getUsedByModules().size() > 0) {
          String msg =
            String.format("Cannot delete module %s: other modules depend on it. Delete them first", module);
          throw new DatasetModuleConflictException(msg);
        }

        Collection<DatasetSpecification> instances = datasetInstanceTable.getByTypes(
          datasetModuleId.getParent(), ImmutableSet.copyOf(module.getTypes()));

        // cannot delete when there's instance that uses it
        if (!instances.isEmpty()) {
          String msg =
            String.format("Cannot delete module %s: other instances depend on it. Delete them first", module);
          throw new DatasetModuleConflictException(msg);
        }

        // remove it from "usedBy" from other modules
        for (String usedModuleName : module.getUsesModules()) {
          DatasetModuleId usedModuleId = new DatasetModuleId(datasetModuleId.getNamespace(), usedModuleName);
          // not using getModuleWithFallback here because we want to know the namespace in which usedModule was found,
          // so we can overwrite it in the MDS in the appropriate namespace
          DatasetModuleMeta usedModule = datasetTypeTable.getModule(usedModuleId);
          // if the usedModule is not found in the current namespace, try finding it in the system namespace
          if (usedModule == null) {
            usedModuleId = NamespaceId.SYSTEM.datasetModule(usedModuleName);
            usedModule = datasetTypeTable.getModule(usedModuleId);
            Preconditions.checkState(usedModule != null, "Could not find a module %s that the module %s uses.",
                                     usedModuleName, datasetModuleId.getEntityName());
          }
          usedModule.removeUsedByModule(datasetModuleId.getEntityName());
          datasetTypeTable.writeModule(usedModuleId.getParent(), usedModule);
        }

        datasetTypeTable.deleteModule(datasetModuleId);
        try {
          // Also delete module jar
          Location moduleJarLocation =
            impersonator.doAs(datasetModuleId,
                              () -> Locations.getLocationFromAbsolutePath(locationFactory,
                                                                          module.getJarLocationPath()));
          if (!moduleJarLocation.delete()) {
            LOG.debug("Could not delete dataset module archive");
          }
        } catch (Exception e) {
          // the only checked exception the try-catch throws is IOException
          Throwables.propagateIfInstanceOf(e, IOException.class);
          throw Throwables.propagate(e);
        }

        return true;
      });
    } catch (RuntimeException e) {
      for (Throwable cause : Throwables.getCausalChain(e)) {
        if (cause instanceof DatasetModuleConflictException) {
          throw (DatasetModuleConflictException) cause;
        }
      }
      throw Throwables.propagate(e);
    } catch (Exception e) {
      LOG.error("Operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Deletes all modules in a namespace, other than system.
   * Presumes that the namespace has already been checked to be non-system.
   *
   * @param namespaceId the {@link NamespaceId} to delete modules from.
   */
  public void deleteModules(final NamespaceId namespaceId) throws DatasetModuleConflictException {
    Preconditions.checkArgument(namespaceId != null && !NamespaceId.SYSTEM.equals(namespaceId),
                                "Cannot delete modules from system namespace");
    LOG.info("Deleting all modules from namespace {}", namespaceId);
    try {
      TransactionRunners.run(transactionRunner, context -> {
        final DatasetTypeTable datasetTypeTable = DatasetTypeTable.create(context);
        final DatasetInstanceTable datasetInstanceTable = new DatasetInstanceTable(context);
        final Set<String> typesToDelete = new HashSet<String>();
        final List<Location> moduleLocations = new ArrayList<>();
        final Collection<DatasetModuleMeta> modules = datasetTypeTable.getModules(namespaceId);
        try {
          impersonator.doAs(namespaceId, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              for (DatasetModuleMeta module : modules) {
                typesToDelete.addAll(module.getTypes());
                moduleLocations.add(Locations.getLocationFromAbsolutePath(locationFactory,
                                                                          module.getJarLocationPath()));
              }
              return null;
            }
          });
        } catch (Exception e) {
          // the callable throws no checked exceptions
          throw Throwables.propagate(e);
        }

        // check if there are any instances that use types of these modules?
        Collection<DatasetSpecification> instances = datasetInstanceTable.getByTypes(namespaceId, typesToDelete);
        // cannot delete when there's instance that uses it
        if (!instances.isEmpty()) {
          throw new DatasetModuleConflictException(
            "Cannot delete all modules: existing dataset instances depend on it. Delete them first"
          );
        }

        datasetTypeTable.deleteModules(namespaceId);
        // Delete module locations
        for (Location moduleLocation : moduleLocations) {
          if (!moduleLocation.delete()) {
            LOG.debug("Could not delete dataset module archive - {}", moduleLocation);
          }
        }
      });
    } catch (RuntimeException e) {
      for (Throwable cause : Throwables.getCausalChain(e)) {
        if (cause instanceof DatasetModuleConflictException) {
          throw (DatasetModuleConflictException) cause;
        }
      }
      LOG.error("Failed to delete all modules from namespace {}", namespaceId);
      throw Throwables.propagate(e);
    } catch (Exception e) {
      LOG.error("Operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  private class DependencyTrackingRegistry implements DatasetDefinitionRegistry {

    private final DatasetTypeTable datasetTypeTable;
    private final boolean tolerateConflicts;
    private final InMemoryDatasetDefinitionRegistry registry;
    private final DatasetModuleId moduleBeingAdded;
    private final List<String> types = Lists.newArrayList();
    private final Set<DatasetTypeId> usedTypes = new LinkedHashSet<>();
    private final ClassLoader classLoader;

    private DependencyTrackingRegistry(DatasetModuleId moduleBeingAdded, DatasetTypeTable datasetTypeTable,
                                       @Nullable ClassLoader classLoader, boolean tolerateConflicts) {
      this.moduleBeingAdded = moduleBeingAdded;
      this.datasetTypeTable = datasetTypeTable;
      this.tolerateConflicts = tolerateConflicts;
      this.registry = new InMemoryDatasetDefinitionRegistry();
      this.classLoader = classLoader;
    }

    List<String> getTypes() {
      return types;
    }

    Set<DatasetTypeId> getUsedTypes() {
      return usedTypes;
    }

    public NamespaceId getNamespaceId() {
      return new NamespaceId(moduleBeingAdded.getNamespace());
    }

    @Override
    public void add(DatasetDefinition def) {
      String typeName = def.getName();
      DatasetTypeId typeId = getNamespaceId().datasetType(typeName);
      DatasetTypeMeta existingType;
      try {
        existingType = datasetTypeTable.getType(typeId);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (existingType != null) {
        DatasetModuleMeta existingModule = existingType.getModules().get(existingType.getModules().size() - 1);
        // we allow redefining an existing type if
        // - it was previously defined by the same module (i.e., this is an upgrade of that module)
        // - it is a forced update and the existing type is not a system type
        if (!moduleBeingAdded.getEntityName().equals(existingModule.getName())
          && (!tolerateConflicts || NamespaceId.SYSTEM.getNamespace().equals(existingModule.getName()))) {
          throw new TypeConflictException(String.format(
            "Attempt to add dataset module '%s' containing dataset type '%s' that already exists in module '%s'",
            moduleBeingAdded.getEntityName(), typeName, existingModule.getName()));
        }
      }
      types.add(typeName);
      registry.add(def);
    }

    @Override
    public <T extends DatasetDefinition> T get(String datasetTypeName) {
      // Find the typeMeta for the type from the right namespace
      DatasetTypeId datasetTypeId = moduleBeingAdded.getParent().datasetType(datasetTypeName);
      DatasetTypeMeta typeMeta = null;
      try {
        typeMeta = datasetTypeTable.getType(datasetTypeId);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (typeMeta == null) {
        // not found in the user namespace. Try finding in the system namespace
        datasetTypeId = NamespaceId.SYSTEM.datasetType(datasetTypeName);
        try {
          typeMeta = datasetTypeTable.getType(datasetTypeId);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (typeMeta == null) {
          // not found in the user namespace as well as system namespace. Bail out.
          throw new IllegalArgumentException("Requested dataset type is not available: " + datasetTypeName);
        }
      }

      if (!registry.hasType(datasetTypeName)) {
        for (DatasetModuleMeta moduleMeta : typeMeta.getModules()) {
          try {
            DatasetDefinitionRegistries.register(moduleMeta.getClassName(), classLoader, registry);
          } catch (TypeConflictException e) {
            // A type may have multiple transitive dependencies on the same module. In that case, it would be
            // better to avoid duplicate registering of the same module. However, we don't know what modules
            // are registered because modules only register their types and but not the module id. So here we
            // just assume that this may happen if the module was already loaded.
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }
      T def = registry.get(datasetTypeName);
      // Here, datasetTypeId has the right namespace (either user or system) where the type was found.
      usedTypes.add(datasetTypeId);
      return def;
    }

    @Override
    public boolean hasType(String datasetTypeName) {
      boolean hasType;
      try {
        hasType = registry.hasType(datasetTypeName) ||
          datasetTypeTable.getType(getNamespaceId().datasetType(datasetTypeName)) != null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return hasType;
    }
  }
}
