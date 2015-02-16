/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.type;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasets;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.TypeConflictException;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.data2.dataset2.tx.TxCallable;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Manages dataset types and modules metadata
 */
public class DatasetTypeManager extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeManager.class);

  private final MDSDatasetsRegistry mdsDatasets;
  private final LocationFactory locationFactory;

  private final Map<String, DatasetModule> defaultModules;

  @Inject
  public DatasetTypeManager(MDSDatasetsRegistry mdsDatasets,
                            LocationFactory locationFactory,
                            @Named("defaultDatasetModules")
                            Map<String, ? extends DatasetModule> defaultModules) {
    this.mdsDatasets = mdsDatasets;
    this.locationFactory = locationFactory;
    this.defaultModules = Maps.newLinkedHashMap(defaultModules);
  }

  @Override
  protected void startUp() throws Exception {
    deployDefaultModules();
  }

  @Override
  protected void shutDown() throws Exception {
    // do nothing
  }

  /**
   * Add datasets module in a namespace
   *
   * @param datasetModuleId the {@link Id.DatasetModule} to add
   * @param className module class
   * @param jarLocation location of the module jar
   */
  public void addModule(final Id.DatasetModule datasetModuleId, final String className, final Location jarLocation)
    throws DatasetModuleConflictException {

    LOG.info("adding module: {}, className: {}, jarLocation: {}",
             datasetModuleId, className, jarLocation == null ? "[local]" : jarLocation.toURI());

    try {
      mdsDatasets.execute(new TxCallable<MDSDatasets, Void>() {
        @Override
        public Void call(MDSDatasets datasets) throws DatasetModuleConflictException {
          DatasetModuleMeta existing = datasets.getTypeMDS().getModule(datasetModuleId);
          if (existing != null) {
            String msg = String.format("cannot add module %s, module with the same name already exists: %s",
                                       datasetModuleId, existing);
            LOG.warn(msg);
            throw new DatasetModuleConflictException(msg);
          }

          ClassLoader cl;
          DatasetModule module;
          File unpackedLocation = Files.createTempDir();
          DependencyTrackingRegistry reg;
          try {
            // NOTE: if jarLocation is null, we assume that this is a system module, ie. always present in classpath
            if (jarLocation != null) {
              BundleJarUtil.unpackProgramJar(jarLocation, unpackedLocation);
            }
            cl = jarLocation == null ? this.getClass().getClassLoader() :
              ProgramClassLoader.create(unpackedLocation, getClass().getClassLoader());
            @SuppressWarnings("unchecked")
            Class clazz = ClassLoaders.loadClass(className, cl, this);
            module = DatasetModules.getDatasetModule(clazz);
            reg = new DependencyTrackingRegistry(datasetModuleId.getNamespace(), datasets);
            module.register(reg);
          } catch (Exception e) {
            LOG.error("Could not instantiate instance of dataset module class {} for module {} using jarLocation {}",
                      className, datasetModuleId, jarLocation);
            throw Throwables.propagate(e);
          } finally {
            try {
              DirUtils.deleteDirectoryContents(unpackedLocation);
            } catch (IOException e) {
              LOG.warn("Failed to delete directory {}", unpackedLocation, e);
            }
          }
          // NOTE: we use set to avoid duplicated dependencies
          // NOTE: we use LinkedHashSet to preserve order in which dependencies must be loaded
          Set<String> moduleDependencies = Sets.newLinkedHashSet();
          for (Id.DatasetType usedType : reg.getUsedTypes()) {
            DatasetModuleMeta usedModule = datasets.getTypeMDS().getModuleByType(usedType);
            Preconditions.checkState(usedModule != null,
                                     String.format("Found a null used module for type %s for while adding module %s",
                                                   usedType, datasetModuleId));
            // adding all used types and the module itself, in this very order to keep the order of loading modules
            // for instantiating a type
            moduleDependencies.addAll(usedModule.getUsesModules());
            boolean added = moduleDependencies.add(usedModule.getName());
            if (added) {
              // also adding this module as a dependent for all modules it uses
              usedModule.addUsedByModule(datasetModuleId.getId());
              datasets.getTypeMDS().writeModule(datasetModuleId.getNamespace(), usedModule);
            }
          }

          DatasetModuleMeta moduleMeta = new DatasetModuleMeta(datasetModuleId.getId(), className,
                                                               jarLocation == null ? null : jarLocation.toURI(),
                                                               reg.getTypes(), Lists.newArrayList(moduleDependencies));
          datasets.getTypeMDS().writeModule(datasetModuleId.getNamespace(), moduleMeta);

          return null;
        }
      });

    } catch (TransactionFailureException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        if (cause instanceof DatasetModuleConflictException) {
          throw (DatasetModuleConflictException) cause;
        } else if (cause instanceof TypeConflictException) {
          throw new DatasetModuleConflictException(cause);
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
   * @param namespaceId the {@link Id.Namespace} to retrieve types from
   * @return collection of types available in the specified namespace
   */
  public Collection<DatasetTypeMeta> getTypes(final Id.Namespace namespaceId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Collection<DatasetTypeMeta>>() {
      @Override
      public Collection<DatasetTypeMeta> call(MDSDatasets datasets) throws DatasetModuleConflictException {
        return datasets.getTypeMDS().getTypes(namespaceId);
      }
    });
  }

  /**
   * Get dataset type information
   * @param datasetTypeId name of the type to get info for
   * @return instance of {@link DatasetTypeMeta} or {@code null} if type
   *         does NOT exist
   */
  @Nullable
  public DatasetTypeMeta getTypeInfo(final Id.DatasetType datasetTypeId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, DatasetTypeMeta>() {
      @Override
      public DatasetTypeMeta call(MDSDatasets datasets) throws DatasetModuleConflictException {
        return datasets.getTypeMDS().getType(datasetTypeId);
      }
    });
  }

  /**
   * @param namespaceId {@link Id.Namespace} to retrieve the module list from
   * @return list of dataset modules information from the specified namespace
   */
  public Collection<DatasetModuleMeta> getModules(final Id.Namespace namespaceId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Collection<DatasetModuleMeta>>() {
      @Override
      public Collection<DatasetModuleMeta> call(MDSDatasets datasets) throws Exception {
        return datasets.getTypeMDS().getModules(namespaceId);
      }
    });
  }

  /**
   * @param datasetModuleId {@link Id.DatasetModule} of the module to return info for
   * @return dataset module info or {@code null} if module with given name does NOT exist
   */
  @Nullable
  public DatasetModuleMeta getModule(final Id.DatasetModule datasetModuleId) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, DatasetModuleMeta>() {
      @Override
      public DatasetModuleMeta call(MDSDatasets datasets) throws Exception {
        return datasets.getTypeMDS().getModule(datasetModuleId);
      }
    });
  }

  /**
   * Deletes specified dataset module
   * @param datasetModuleId {@link Id.DatasetModule} of the dataset module to delete
   * @return true if deleted successfully, false if module didn't exist: nothing to delete
   * @throws DatasetModuleConflictException when there are other modules depend on the specified one, in which case
   *         deletion does NOT happen
   */
  public boolean deleteModule(final Id.DatasetModule datasetModuleId) throws DatasetModuleConflictException {
    LOG.info("Deleting module {}", datasetModuleId);
    try {
      return mdsDatasets.execute(new TxCallable<MDSDatasets, Boolean>() {
        @Override
        public Boolean call(MDSDatasets datasets) throws DatasetModuleConflictException {
          DatasetModuleMeta module = datasets.getTypeMDS().getModule(datasetModuleId);

          if (module == null) {
            return false;
          }

          // cannot delete when there's module that uses it
          if (module.getUsedByModules().size() > 0) {
            String msg =
              String.format("Cannot delete module %s: other modules depend on it. Delete them first", module);
            throw new DatasetModuleConflictException(msg);
          }

          Collection<DatasetSpecification> dependentInstances =
            datasets.getInstanceMDS().getByTypes(ImmutableSet.copyOf(module.getTypes()));
          // cannot delete when there's instance that uses it
          if (dependentInstances.size() > 0) {
            String msg =
              String.format("Cannot delete module %s: other instances depend on it. Delete them first", module);
            throw new DatasetModuleConflictException(msg);
          }

          // remove it from "usedBy" from other modules
          for (String usedModuleName : module.getUsesModules()) {
            Id.DatasetModule usedModuleId = Id.DatasetModule.from(datasetModuleId.getNamespace(), usedModuleName);
            DatasetModuleMeta usedModule = datasets.getTypeMDS().getModule(usedModuleId);
            // if the usedModule is not found in the current namespace, try finding it in the system namespace
            if (usedModule == null) {
              usedModule = datasets.getTypeMDS().getModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE,
                                                                                 usedModuleName));
              Preconditions.checkState(usedModule != null, "Could not find a module %s that the module %s uses.",
                                       usedModuleName, datasetModuleId.getId());
            }
            usedModule.removeUsedByModule(datasetModuleId.getId());
            datasets.getTypeMDS().writeModule(datasetModuleId.getNamespace(), usedModule);
          }

          datasets.getTypeMDS().deleteModule(datasetModuleId);

          return true;
        }
      });
    } catch (TransactionFailureException e) {
      if (e.getCause() != null && e.getCause() instanceof DatasetModuleConflictException) {
        throw (DatasetModuleConflictException) e.getCause();
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
   * @param namespaceId the {@link Id.Namespace} to delete modules from.
   */
  public void deleteModules(final Id.Namespace namespaceId) throws DatasetModuleConflictException {
    Preconditions.checkArgument(namespaceId != null && !Constants.SYSTEM_NAMESPACE.equals(namespaceId),
                                "Cannot delete modules from system namespace");
    LOG.warn("Deleting all modules from namespace {}", namespaceId);
    try {
      mdsDatasets.execute(new TxCallable<MDSDatasets, Void>() {
        @Override
        public Void call(MDSDatasets datasets) throws DatasetModuleConflictException {
          Set<String> typesToDelete = Sets.newHashSet();
          for (DatasetModuleMeta module : datasets.getTypeMDS().getModules(namespaceId)) {
            typesToDelete.addAll(module.getTypes());
          }

          // check if there are any instances that use types of these modules?
          Collection<DatasetSpecification> dependentInstances = datasets.getInstanceMDS().getByTypes(typesToDelete);
          // cannot delete when there's instance that uses it
          if (dependentInstances.size() > 0) {
            String msg =
              String.format("Cannot delete all modules: existing dataset instances depend on it. Delete them first");
            throw new DatasetModuleConflictException(msg);
          }

          datasets.getTypeMDS().deleteModules(namespaceId);

          return null;
        }
      });
    } catch (TransactionFailureException e) {
      if (e.getCause() != null && e.getCause() instanceof DatasetModuleConflictException) {
        throw (DatasetModuleConflictException) e.getCause();
      }
      LOG.error("Failed to delete all modules from namespace {}", namespaceId);
      throw Throwables.propagate(e);
    } catch (Exception e) {
      LOG.error("Operation failed", e);
      throw Throwables.propagate(e);
    }
  }

  private void deployDefaultModules() {
    // adding default modules to be available in dataset manager service
    for (Map.Entry<String, DatasetModule> module : defaultModules.entrySet()) {
      try {
        // NOTE: we assume default modules are always in classpath, hence passing null for jar location
        // NOTE: we add default modules in the system namespace
        Id.DatasetModule defaultModule = Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, module.getKey());
        addModule(defaultModule, module.getValue().getClass().getName(), null);
      } catch (DatasetModuleConflictException e) {
        // perfectly fine: we need to add default modules only the very first time service is started
        LOG.info("Not adding " + module.getKey() + " module: it already exists");
      } catch (Throwable th) {
        LOG.error("Failed to add {} module. Aborting.", module.getKey(), th);
        throw Throwables.propagate(th);
      }
    }
  }

  private class DependencyTrackingRegistry implements DatasetDefinitionRegistry {
    private final MDSDatasets datasets;
    private final InMemoryDatasetDefinitionRegistry registry;
    private final Id.Namespace namespaceId;

    private final List<String> types = Lists.newArrayList();
    private final LinkedHashSet<Id.DatasetType> usedTypes = Sets.newLinkedHashSet();

    public DependencyTrackingRegistry(Id.Namespace namespaceId, MDSDatasets datasets) {
      this.namespaceId = namespaceId;
      this.datasets = datasets;
      this.registry = new InMemoryDatasetDefinitionRegistry();
    }

    public List<String> getTypes() {
      return types;
    }

    public Set<Id.DatasetType> getUsedTypes() {
      return usedTypes;
    }

    public Id.Namespace getNamespaceId() {
      return namespaceId;
    }

    @Override
    public void add(DatasetDefinition def) {
      String typeName = def.getName();
      if (datasets.getTypeMDS().getType(Id.DatasetType.from(namespaceId, typeName)) != null) {
        String msg = "Cannot add dataset type: it already exists: " + typeName;
        LOG.error(msg);
        throw new TypeConflictException(msg);
      }
      types.add(typeName);
      registry.add(def);
    }

    @Override
    public <T extends DatasetDefinition> T get(String datasetTypeName) {
      T def;
      Id.DatasetType datasetTypeId = Id.DatasetType.from(namespaceId, datasetTypeName);
      if (registry.hasType(datasetTypeName)) {
        def = registry.get(datasetTypeName);
      } else {
        DatasetTypeMeta typeMeta = datasets.getTypeMDS().getType(datasetTypeId);
        if (typeMeta == null) {
          // not found in the user namespace. Try finding in the system namespace
          datasetTypeId = Id.DatasetType.from(Constants.SYSTEM_NAMESPACE, datasetTypeName);
          typeMeta = datasets.getTypeMDS().getType(datasetTypeId);
          if (typeMeta == null) {
            // not found in the user namespace as well as system namespace. Bail out.
            throw new IllegalArgumentException("Requested dataset type is not available: " + datasetTypeName);
          }
        }
        try {
          def = new DatasetDefinitionLoader(locationFactory).load(typeMeta, registry);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      // Here, datasetTypeId has the right namespace (either user or system) where the type was found.
      usedTypes.add(datasetTypeId);
      return def;
    }

    @Override
    public boolean hasType(String datasetTypeName) {
      return datasets.getTypeMDS().getType(Id.DatasetType.from(namespaceId, datasetTypeName)) != null;
    }
  }
}
