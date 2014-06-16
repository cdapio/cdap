package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.lang.jar.JarClassLoader;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.client.DatasetServiceClient;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.DatasetNamespace;
import com.continuuity.data2.dataset2.SingleTypeModule;
import com.continuuity.data2.dataset2.module.lib.DatasetModules;
import com.continuuity.internal.lang.ClassLoaders;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * {@link com.continuuity.data2.dataset2.DatasetFramework} implementation that talks to DatasetFramework Service
 */
public class RemoteDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetFramework.class);

  private final DatasetServiceClient client;
  private final Map<String, DatasetModule> modulesCache;
  private final DatasetDefinitionRegistry registry;
  private final LocationFactory locationFactory;
  private final DatasetNamespace namespace;

  @Inject
  public RemoteDatasetFramework(DatasetServiceClient client,
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
  public void addModule(String moduleName, DatasetModule module)
    throws DatasetManagementException {

    // We support easier APIs for custom datasets: user can implement dataset and make it available for others to use
    // by only implementing Dataset. Without requiring implementing datasets module, definition and other classes.
    // In this case we wrap that Dataset implementation with SingleTypeModule. But since we don't have a way to serde
    // dataset modules, if we pass only SingleTypeModule.class the Dataset implementation info will be lost. Hence, as
    // a workaround we put Dataset implementation class in MDS (on DatasetService) and wrapping it with SingleTypeModule
    // when we need to instantiate module.
    //
    // todo: do proper serde for modules instead of just passing class name to server
    Class<?> typeClass;
    if (module instanceof SingleTypeModule) {
      typeClass = ((SingleTypeModule) module).getDataSetClass();
    } else {
      typeClass = module.getClass();
    }

    addModule(moduleName, typeClass);
  }

  @Override
  public void deleteModule(String moduleName) throws DatasetManagementException {
    client.deleteModule(moduleName);
  }

  @Override
  public void addInstance(String datasetType, String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException {

    client.addInstance(namespace(datasetInstanceName), datasetType, props);
  }

  @Override
  public Collection<String> getInstances() throws DatasetManagementException {
    Collection<DatasetSpecification> allInstances = client.getAllInstances();
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (DatasetSpecification spec : allInstances) {
      builder.add(spec.getName());
    }
    return builder.build();
  }

  @Override
  public boolean hasInstance(String instanceName) throws DatasetManagementException {
    return client.getInstance(namespace(instanceName)) != null;
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

  private void addModule(String moduleName, Class<?> typeClass) throws DatasetManagementException {
    Location tempJarPath;
    if (typeClass.getClassLoader() instanceof JarClassLoader) {
      // for auto-registering module with application jar deploy
      tempJarPath = ((JarClassLoader) typeClass.getClassLoader()).getLocation();
    } else {
      tempJarPath = new LocalLocationFactory().create(JarFinder.getJar(typeClass));
    }

    client.addModule(moduleName, typeClass.getName(), tempJarPath);
  }

  private String namespace(String datasetInstanceName) {
    return namespace.namespace(datasetInstanceName);
  }

  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service
  public <T extends DatasetDefinition> T getDatasetDefinition(DatasetTypeMeta implementationInfo,
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
          module = DatasetModules.getDatasetModule(moduleClass);
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
