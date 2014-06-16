package com.continuuity.data2.datafabric.dataset.type;

import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.common.lang.jar.JarClassLoader;
import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.module.lib.DatasetModules;
import com.continuuity.data2.transaction.DefaultTransactionExecutor;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.lang.ClassLoaders;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Manages dataset types and modules metadata
 */
// todo: there's ugly work with Datasets & Transactions (incl. exceptions inside txnl code) which will be revised as
//       part of open-sourcing Datasets effort
public class DatasetTypeManager extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeManager.class);

  private final TransactionSystemClient txClient;
  private final DatasetFramework mdsDatasetFramework;
  private final LocationFactory locationFactory;

  private final Map<String, DatasetModule> defaultModules;

  /** dataset types metadata store */
  private DatasetTypeMDS mds;
  private TransactionAware txAware;

  /**
   * Ctor
   * @param mdsDatasetFramework dataset manager to be used to access the metadata store
   * @param txSystemClient tx client to be used to operate on the metadata store
   */
  public DatasetTypeManager(DatasetFramework mdsDatasetFramework,
                            TransactionSystemClient txSystemClient,
                            LocationFactory locationFactory,
                            Map<String, DatasetModule> defaultModules) {
    this.mdsDatasetFramework = mdsDatasetFramework;
    this.txClient = txSystemClient;
    this.locationFactory = locationFactory;
    this.defaultModules = Maps.newLinkedHashMap(defaultModules);
  }

  @Override
  protected void startUp() throws Exception {
    // "null" for class being in system classpath, for mds it is always true
    OrderedTable table = DatasetsUtil.getOrCreateDataset(mdsDatasetFramework, "datasets.type", "orderedTable",
                                                         DatasetProperties.EMPTY, null);
    this.txAware = (TransactionAware) table;
    this.mds = new DatasetTypeMDS(table);

    deployDefaultModules();
  }

  @Override
  protected void shutDown() throws Exception {
    mds.close();
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
      getTxExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws DatasetModuleConflictException {
          DatasetModuleMeta existing = mds.getModule(name);
          if (existing != null) {
            String msg = String.format("cannot add module %s, module with the same name already exists: %s",
                                       name, existing);
            LOG.warn(msg);
            throw new DatasetModuleConflictException(msg);
          }

          ClassLoader cl;
          DatasetModule module;
          try {
            // NOTE: we assume all classes needed to load dataset module class are available in the jar or otherwise
            //       are system classes
            // NOTE: if jarLocation is null, we assume that this is a system module, ie. always present in classpath
            cl = jarLocation == null ? this.getClass().getClassLoader() : new JarClassLoader(jarLocation);
            @SuppressWarnings("unchecked")
            Class clazz = ClassLoaders.loadClass(className, cl, this);
            module = DatasetModules.getDatasetModule(clazz);
          } catch (Exception e) {
            LOG.error("Could not instantiate instance of dataset module class {} for module {} using jarLocation {}",
                      className, name, jarLocation);
            throw Throwables.propagate(e);
          }

          DependencyTrackingRegistry reg = new DependencyTrackingRegistry(cl);
          module.register(reg);

          List<String> moduleDependencies = Lists.newArrayList();
          for (String usedType : reg.getUsedTypes()) {
            DatasetModuleMeta usedModule = mds.getModuleByType(usedType);
            // adding all used types and the module itself, in this very order to keep the order of loading modules
            // for instantiating a type
            moduleDependencies.addAll(usedModule.getUsesModules());
            moduleDependencies.add(usedModule.getName());
            // also adding this module as a dependent for all modules it uses
            usedModule.addUsedByModule(name);
            mds.write(usedModule);
          }

          DatasetModuleMeta moduleMeta = new DatasetModuleMeta(name, className,
                                                               jarLocation == null ? null : jarLocation.toURI(),
                                                               reg.getTypes(), moduleDependencies);
          mds.write(moduleMeta);
        }

      });
    } catch (TransactionFailureException e) {
      if (e.getCause() != null && e.getCause() instanceof DatasetModuleConflictException) {
        throw (DatasetModuleConflictException) e.getCause();
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return collection of types available in the system
   */
  public Collection<DatasetTypeMeta> getTypes() {
    return getTxExecutor().executeUnchecked(new Callable<Collection<DatasetTypeMeta>>() {
      @Override
      public Collection<DatasetTypeMeta> call() throws Exception {
        return mds.getTypes();
      }
    });
  }

  /**
   * Get dataset type information
   * @param typeName name of the type to get info for
   * @return instance of {@link com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta} or {@code null} if type
   *         does NOT exist
   */
  @Nullable
  public DatasetTypeMeta getTypeInfo(final String typeName) {
    return getTxExecutor().executeUnchecked(new Callable<DatasetTypeMeta>() {
      @Override
      public DatasetTypeMeta call() throws Exception {
        return mds.getType(typeName);
      }
    });
  }

  /**
   * @return list of dataset modules information
   */
  public Collection<DatasetModuleMeta> getModules() {
    return getTxExecutor().executeUnchecked(new Callable<Collection<DatasetModuleMeta>>() {
      @Override
      public Collection<DatasetModuleMeta> call() throws Exception {
        return mds.getModules();
      }
    });
  }

  /**
   * @param name of the module to return info for
   * @return dataset module info or {@code null} if module with given name does NOT exist
   */
  @Nullable
  public DatasetModuleMeta getModule(final String name) {
    return getTxExecutor().executeUnchecked(new Callable<DatasetModuleMeta>() {
      @Override
      public DatasetModuleMeta call() throws Exception {
        return mds.getModule(name);
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
      return getTxExecutor().execute(new Callable<Boolean>() {
        @Override
        public Boolean call() throws DatasetModuleConflictException {
          DatasetModuleMeta module = mds.getModule(name);

          if (module == null) {
            return false;
          }

          // cannot delete when there's module that uses it
          if (module.getUsedByModules().size() > 0) {
            String msg =
              String.format("Cannot delete module %s: other modules depend on it. Delete them first", module);
            throw new DatasetModuleConflictException(msg);
          }

          // remove it from "usedBy" from other modules
          for (String usedModuleName : module.getUsesModules()) {
            DatasetModuleMeta usedModule = mds.getModule(usedModuleName);
            usedModule.removeUsedByModule(name);
            mds.write(usedModule);
          }

          mds.deleteModule(name);

          return true;
        }
      });
    } catch (TransactionFailureException e) {
      if (e.getCause() != null && e.getCause() instanceof DatasetModuleConflictException) {
        throw (DatasetModuleConflictException) e.getCause();
      }
      throw Throwables.propagate(e);
    }
  }

  /**
   * Deletes all modules (apart from default).
   */
  public void deleteModules() {
    LOG.warn("Deleting all modules apart {}", Joiner.on(", ").join(defaultModules.keySet()));
    try {
      getTxExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws DatasetModuleConflictException {
          for (DatasetModuleMeta module : mds.getModules()) {
            if (!defaultModules.containsKey(module.getName())) {
              mds.deleteModule(module.getName());
            }
          }
        }
      });
    } catch (TransactionFailureException e) {
      LOG.error("Failed to do a delete of all modules apart {}", Joiner.on(", ").join(defaultModules.keySet()));
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

  private TransactionExecutor getTxExecutor() {
    return new DefaultTransactionExecutor(txClient, ImmutableList.of(txAware));
  }

  private class DependencyTrackingRegistry implements DatasetDefinitionRegistry {
    private final DatasetDefinitionRegistry registry;
    private final ClassLoader classLoader;

    private final List<String> types = Lists.newArrayList();
    private final List<String> usedTypes = Lists.newArrayList();

    public DependencyTrackingRegistry(ClassLoader classLoader) {
      this.classLoader = classLoader;
      this.registry = new InMemoryDatasetDefinitionRegistry();
    }

    public List<String> getTypes() {
      return types;
    }

    public List<String> getUsedTypes() {
      return usedTypes;
    }

    @Override
    public void add(DatasetDefinition def) {
      types.add(def.getName());
      registry.add(def);
    }

    @Override
    public <T extends DatasetDefinition> T get(String datasetTypeName) {
      T def = registry.get(datasetTypeName);
      if (def == null) {
        DatasetTypeMeta typeMeta = mds.getType(datasetTypeName);
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

    public ClassLoader getClassLoader() {
      return classLoader;
    }
  }
}
