package com.continuuity.data2.datafabric.dataset;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.lang.ClassLoaders;
import com.continuuity.common.lang.jar.JarClassLoader;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.data2.datafabric.dataset.service.DatasetInstanceMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.dataset2.SingleTypeModule;
import com.continuuity.data2.dataset2.module.lib.DatasetModules;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * {@link com.continuuity.data2.dataset2.DatasetFramework} implementation that talks to DatasetFramework Service
 */
public class RemoteDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetFramework.class);

  private final DatasetServiceClient client;
  private final DatasetDefinitionRegistryFactory registryFactory;
  private final LocationFactory locationFactory;
  private final DatasetTypeClassLoaderFactory typeLoader;

  @Inject
  public RemoteDatasetFramework(DiscoveryServiceClient discoveryClient,
                                LocationFactory locationFactory,
                                DatasetDefinitionRegistryFactory registryFactory,
                                DatasetTypeClassLoaderFactory typeLoader) {

    this.client = new DatasetServiceClient(discoveryClient);
    this.locationFactory = locationFactory;
    this.registryFactory = registryFactory;
    this.typeLoader = typeLoader;
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
  public void deleteAllModules() throws DatasetManagementException {
    client.deleteModules();
  }

  @Override
  public void addInstance(String datasetType, String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException {

    client.addInstance(datasetInstanceName, datasetType, props);
  }

  @Override
  public Collection<DatasetSpecification> getInstances() throws DatasetManagementException {
    return client.getAllInstances();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(String name) throws DatasetManagementException {
    DatasetInstanceMeta meta = client.getInstance(name);
    return meta == null ? null : meta.getSpec();
  }

  @Override
  public boolean hasInstance(String instanceName) throws DatasetManagementException {
    return client.getInstance(instanceName) != null;
  }

  @Override
  public boolean hasType(String typeName) throws DatasetManagementException {
    return client.getType(typeName) != null;
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException {
    client.deleteInstance(datasetInstanceName);
  }

  @Override
  public void deleteAllInstances() throws DatasetManagementException, IOException {
    client.deleteInstances();
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    DatasetInstanceMeta instanceInfo = client.getInstance(datasetInstanceName);
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getDatasetType(instanceInfo.getType(), classLoader);
    return (T) type.getAdmin(instanceInfo.getSpec());
  }

  @Override
  public <T extends Dataset> T getDataset(String datasetInstanceName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    DatasetInstanceMeta instanceInfo = client.getInstance(datasetInstanceName);
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getDatasetType(instanceInfo.getType(), classLoader);
    return (T) type.getDataset(instanceInfo.getSpec());
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

  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service
  public <T extends DatasetType> T getDatasetType(DatasetTypeMeta implementationInfo,
                                                  ClassLoader classLoader)
    throws DatasetManagementException {


    if (classLoader == null) {
      classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader());
    }

    DatasetDefinitionRegistry registry = registryFactory.create();
    List<DatasetModuleMeta> modulesToLoad = implementationInfo.getModules();
    for (DatasetModuleMeta moduleMeta : modulesToLoad) {
      // adding dataset module jar to classloader
      try {
        classLoader = typeLoader.create(moduleMeta, classLoader);
      } catch (IOException e) {
        LOG.error("Was not able to init classloader for module {} while trying to load type {}",
                  moduleMeta, implementationInfo, e);
        throw Throwables.propagate(e);
      }

      Class<?> moduleClass;

      // try program class loader then reactor class loader
      try {
        moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), classLoader, this);
      } catch (ClassNotFoundException e) {
        try {
          moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), null, this);
        } catch (ClassNotFoundException e2) {
          LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                    moduleMeta.getClassName(), implementationInfo, e);
          throw Throwables.propagate(e);
        }
      }

      try {
        DatasetModule module = DatasetModules.getDatasetModule(moduleClass);
        module.register(registry);
      } catch (Exception e) {
        LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                  moduleMeta.getClassName(), implementationInfo, e);
        throw Throwables.propagate(e);
      }
    }

    return (T) new DatasetType(registry.get(implementationInfo.getName()), classLoader);
  }
}
