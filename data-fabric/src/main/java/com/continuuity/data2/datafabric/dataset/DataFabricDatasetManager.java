package com.continuuity.data2.datafabric.dataset;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.client.DatasetManagerServiceClient;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.DatasetNamespace;
import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.lang.jar.JarClassLoader;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.internal.lang.ClassLoaders;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link DatasetManager} implementation that talks to DatasetManager Service
 */
public class DataFabricDatasetManager implements DatasetManager {
  private static final Logger LOG = LoggerFactory.getLogger(DataFabricDatasetManager.class);

  private final DatasetManagerServiceClient client;
  private final Map<String, DatasetModule> modulesCache;
  private final DatasetDefinitionRegistry registry;
  private final LocationFactory locationFactory;
  private final DatasetNamespace namespace;

  @Inject
  public DataFabricDatasetManager(DatasetManagerServiceClient client,
                                  CConfiguration conf,
                                  LocationFactory locationFactory,
                                  DatasetDefinitionRegistry registry) {

    this.client = client;
    this.modulesCache = Maps.newHashMap();
    this.locationFactory = locationFactory;
    this.registry = registry;
    this.namespace = new ReactorDatasetNamespace(conf, DataSetAccessor.Namespace.USER);
  }

  @Override
  public void register(String moduleName, Class<? extends DatasetModule> module)
    throws DatasetManagementException {

    Location tempJarPath;
    if (module.getClassLoader() instanceof JarClassLoader) {
      // for auto-registering module with application jar deploy
      tempJarPath = ((JarClassLoader) module.getClassLoader()).getLocation();
    } else {
      tempJarPath = new LocalLocationFactory().create(JarFinder.getJar(module));
    }

    client.addModule(moduleName, module.getName(), tempJarPath);
  }

  @Override
  public void deleteModule(String moduleName) throws DatasetManagementException {
    client.deleteModule(moduleName);
  }

  @Override
  public void addInstance(String datasetType, String datasetInstanceName, DatasetInstanceProperties props)
    throws DatasetManagementException {

    client.addInstance(namespace(datasetInstanceName), datasetType, props);
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    client.deleteInstance(namespace(datasetInstanceName));
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    DatasetInstanceMeta instanceInfo = client.getInstance(namespace(datasetInstanceName));
    if (instanceInfo == null) {
      return null;
    }
    DatasetDefinition impl = getDatasetDefinition(instanceInfo.getType(), classLoader);

    return (T) impl.getAdmin(instanceInfo.getSpec());
  }

  @Override
  public <T extends Dataset> T getDataset(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    DatasetInstanceMeta instanceInfo = client.getInstance(namespace(datasetInstanceName));
    if (instanceInfo == null) {
      return null;
    }
    DatasetDefinition impl = getDatasetDefinition(instanceInfo.getType(), classLoader);

    return (T) impl.getDataset(instanceInfo.getSpec());
  }

  private String namespace(String datasetInstanceName) {
    return namespace.namespace(datasetInstanceName);
  }

  private <T extends DatasetDefinition> T getDatasetDefinition(DatasetTypeMeta implementationInfo,
                                                               ClassLoader classLoader)
    throws DatasetManagementException {

    List<DatasetModuleMeta> modulesToLoad = implementationInfo.getModules();
    for (DatasetModuleMeta moduleMeta : modulesToLoad) {
      if (!modulesCache.containsKey(moduleMeta.getName())) {
        if (moduleMeta.getJarLocation() != null) {
          // adding dataset module jar to classloader
          try {
            classLoader = classLoader == null ?
            new JarClassLoader(locationFactory.create(moduleMeta.getJarLocation())) :
            new JarClassLoader(locationFactory.create(moduleMeta.getJarLocation()), classLoader);
          } catch (IOException e) {
            LOG.error("Was not able to init classloader for module {} while trying to load type {}",
                      moduleMeta, implementationInfo, e);
            throw Throwables.propagate(e);
          }
        }
        DatasetModule module;
        try {
          Class<?> moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), classLoader, this);
          module = (DatasetModule) moduleClass.newInstance();
        } catch (Exception e) {
          LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                    moduleMeta.getClassName(), implementationInfo, e);
          throw Throwables.propagate(e);
        }
        modulesCache.put(moduleMeta.getName(), module);
        module.register(registry);
      }
    }

    return registry.get(implementationInfo.getName());
  }
}
