/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
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
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Joiner;
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
  private final boolean allowDatasetUncheckedUpgrade;

  @Inject
  public DatasetTypeManager(CConfiguration configuration, MDSDatasetsRegistry mdsDatasets,
                            LocationFactory locationFactory,
                            @Named("defaultDatasetModules")
                            Map<String, ? extends DatasetModule> defaultModules) {
    this.mdsDatasets = mdsDatasets;
    this.locationFactory = locationFactory;
    this.defaultModules = Maps.newLinkedHashMap(defaultModules);
    this.allowDatasetUncheckedUpgrade = configuration.getBoolean(Constants.Dataset.DATASET_UNCHECKED_UPGRADE);
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
   * Add datasets module
   * @param name module name
   * @param className module class
   * @param jarLocation location of the module jar
   */
  public void addModule(final String name, final String className, final Location jarLocation)
    throws DatasetModuleConflictException {

    LOG.info("adding module, name: {}, className: {}, jarLocation: {}",
             name, className, jarLocation == null ? "[local]" : jarLocation.toURI());

    try {
      mdsDatasets.execute(new TxCallable<MDSDatasets, Void>() {
        @Override
        public Void call(MDSDatasets datasets) throws DatasetModuleConflictException {
          DatasetModuleMeta existing = datasets.getTypeMDS().getModule(name);
          if (existing != null && !allowDatasetUncheckedUpgrade) {
            String msg = String.format("cannot add module %s, module with the same name already exists: %s",
                                       name, existing);
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
            reg = new DependencyTrackingRegistry(datasets);
            module.register(reg);
          } catch (Exception e) {
            LOG.error("Could not instantiate instance of dataset module class {} for module {} using jarLocation {}",
                      className, name, jarLocation);
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
          for (String usedType : reg.getUsedTypes()) {
            DatasetModuleMeta usedModule = datasets.getTypeMDS().getModuleByType(usedType);
            // adding all used types and the module itself, in this very order to keep the order of loading modules
            // for instantiating a type
            moduleDependencies.addAll(usedModule.getUsesModules());
            boolean added = moduleDependencies.add(usedModule.getName());
            if (added) {
              // also adding this module as a dependent for all modules it uses
              usedModule.addUsedByModule(name);
              datasets.getTypeMDS().write(usedModule);
            }
          }

          DatasetModuleMeta moduleMeta = new DatasetModuleMeta(name, className,
                                                               jarLocation == null ? null : jarLocation.toURI(),
                                                               reg.getTypes(), Lists.newArrayList(moduleDependencies));
          datasets.getTypeMDS().write(moduleMeta);

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
   * @return collection of types available in the system
   */
  public Collection<DatasetTypeMeta> getTypes() {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Collection<DatasetTypeMeta>>() {
      @Override
      public Collection<DatasetTypeMeta> call(MDSDatasets datasets) throws DatasetModuleConflictException {
        return datasets.getTypeMDS().getTypes();
      }
    });
  }

  /**
   * Get dataset type information
   * @param typeName name of the type to get info for
   * @return instance of {@link DatasetTypeMeta} or {@code null} if type
   *         does NOT exist
   */
  @Nullable
  public DatasetTypeMeta getTypeInfo(final String typeName) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, DatasetTypeMeta>() {
      @Override
      public DatasetTypeMeta call(MDSDatasets datasets) throws DatasetModuleConflictException {
        return datasets.getTypeMDS().getType(typeName);
      }
    });
  }

  /**
   * @return list of dataset modules information
   */
  public Collection<DatasetModuleMeta> getModules() {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, Collection<DatasetModuleMeta>>() {
      @Override
      public Collection<DatasetModuleMeta> call(MDSDatasets datasets) throws Exception {
        return datasets.getTypeMDS().getModules();
      }
    });
  }

  /**
   * @param name of the module to return info for
   * @return dataset module info or {@code null} if module with given name does NOT exist
   */
  @Nullable
  public DatasetModuleMeta getModule(final String name) {
    return mdsDatasets.executeUnchecked(new TxCallable<MDSDatasets, DatasetModuleMeta>() {
      @Override
      public DatasetModuleMeta call(MDSDatasets datasets) throws Exception {
        return datasets.getTypeMDS().getModule(name);
      }
    });
  }

  /**
   * Deletes specified dataset module
   * @param name name of dataset module to delete
   * @return true if deleted successfully, false if module didn't exist: nothing to delete
   * @throws DatasetModuleConflictException when there are other modules depend on the specified one, in which case
   *         deletion does NOT happen
   */
  public boolean deleteModule(final String name) throws DatasetModuleConflictException {
    LOG.info("Deleting module {}", name);
    try {
      return mdsDatasets.execute(new TxCallable<MDSDatasets, Boolean>() {
        @Override
        public Boolean call(MDSDatasets datasets) throws DatasetModuleConflictException {
          DatasetModuleMeta module = datasets.getTypeMDS().getModule(name);

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
            DatasetModuleMeta usedModule = datasets.getTypeMDS().getModule(usedModuleName);
            usedModule.removeUsedByModule(name);
            datasets.getTypeMDS().write(usedModule);
          }

          datasets.getTypeMDS().deleteModule(name);

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
   * Deletes all modules (apart from default).
   */
  public void deleteModules() throws DatasetModuleConflictException {
    LOG.warn("Deleting all modules apart {}", Joiner.on(", ").join(defaultModules.keySet()));
    try {
      mdsDatasets.execute(new TxCallable<MDSDatasets, Void>() {
        @Override
        public Void call(MDSDatasets datasets) throws DatasetModuleConflictException {
          List<String> modulesToDelete = Lists.newArrayList();
          Set<String> typesToDelete = Sets.newHashSet();
          for (DatasetModuleMeta module : datasets.getTypeMDS().getModules()) {
            if (!defaultModules.containsKey(module.getName())) {
              modulesToDelete.add(module.getName());
              typesToDelete.addAll(module.getTypes());
            }
          }

          // check if there are any instances that use types of these modules?
          Collection<DatasetSpecification> dependentInstances = datasets.getInstanceMDS().getByTypes(typesToDelete);
          // cannot delete when there's instance that uses it
          if (dependentInstances.size() > 0) {
            String msg =
              String.format("Cannot delete all modules: existing dataset instances depend on it. Delete them first");
            throw new DatasetModuleConflictException(msg);
          }

          for (String module : modulesToDelete) {
            datasets.getTypeMDS().deleteModule(module);
          }

          return null;
        }
      });
    } catch (TransactionFailureException e) {
      if (e.getCause() != null && e.getCause() instanceof DatasetModuleConflictException) {
        throw (DatasetModuleConflictException) e.getCause();
      }
      LOG.error("Failed to do a delete of all modules apart {}", Joiner.on(", ").join(defaultModules.keySet()));
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
        addModule(module.getKey(), module.getValue().getClass().getName(), null);
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

    private final List<String> types = Lists.newArrayList();
    private final LinkedHashSet<String> usedTypes = Sets.newLinkedHashSet();

    public DependencyTrackingRegistry(MDSDatasets datasets) {
      this.datasets = datasets;
      this.registry = new InMemoryDatasetDefinitionRegistry();
    }

    public List<String> getTypes() {
      return types;
    }

    public Set<String> getUsedTypes() {
      return usedTypes;
    }

    @Override
    public void add(DatasetDefinition def) {
      String typeName = def.getName();
      if (datasets.getTypeMDS().getType(typeName) != null && !allowDatasetUncheckedUpgrade) {
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
      if (registry.hasType(datasetTypeName)) {
        def = registry.get(datasetTypeName);
      } else {
        DatasetTypeMeta typeMeta = datasets.getTypeMDS().getType(datasetTypeName);
        if (typeMeta == null) {
          throw new IllegalArgumentException("Requested dataset type is not available: " + datasetTypeName);
        }
        try {
          def = new DatasetDefinitionLoader(locationFactory).load(typeMeta, registry);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      usedTypes.add(datasetTypeName);
      return def;
    }

    @Override
    public boolean hasType(String datasetTypeName) {
      return datasets.getTypeMDS().getType(datasetTypeName) != null;
    }
  }
}
