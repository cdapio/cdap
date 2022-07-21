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

package io.cdap.cdap.data2.datafabric.dataset;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistries;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.module.lib.DatasetModules;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

/**
 * {@link io.cdap.cdap.data2.dataset2.DatasetFramework} implementation that talks to DatasetFramework Service
 */
@SuppressWarnings("unchecked")
public class RemoteDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetFramework.class);
  private static final Predicate<Throwable> RETRYABLE_PREDICATE = t ->
    t instanceof RetryableException ||
    (t instanceof UncheckedExecutionException && t.getCause() instanceof RetryableException);

  private final CConfiguration cConf;
  private final LoadingCache<NamespaceId, DatasetServiceClient> clientCache;
  private final DatasetDefinitionRegistryFactory registryFactory;
  private final RetryStrategy retryStrategy;

  @Inject
  public RemoteDatasetFramework(final CConfiguration cConf,
                                DatasetDefinitionRegistryFactory registryFactory,
                                final AuthenticationContext authenticationContext,
                                RemoteClientFactory remoteClientFactory) {
    this.cConf = cConf;
    this.clientCache = CacheBuilder.newBuilder().build(new CacheLoader<NamespaceId, DatasetServiceClient>() {
      @Override
      public DatasetServiceClient load(NamespaceId namespace) throws Exception {
        return new DatasetServiceClient(namespace, cConf, authenticationContext, remoteClientFactory);
      }
    });
    this.registryFactory = registryFactory;
    this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.dataset.remote.");
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module) throws DatasetManagementException {
    Class<?> moduleClass = DatasetModules.getDatasetModuleClass(module);
    try {
      Location deploymentJar = createDeploymentJar(moduleClass);
      try {
        clientCache.getUnchecked(moduleId.getParent())
          .addModule(moduleId.getEntityName(), moduleClass.getName(), deploymentJar);
      } finally {
        try {
          deploymentJar.delete();
        } catch (IOException e) {
          // Just log warning, since the add module operation can still proceed
          LOG.warn("Failed to delete temporary deployment jar {}", deploymentJar, e);
        }
      }
    } catch (IOException e) {
      String msg = String.format("Could not create jar for deploying dataset module %s with main class %s",
                                 moduleId, moduleClass.getName());
      LOG.error(msg, e);
      throw new DatasetManagementException(msg, e);
    }
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module,
                        Location jarLocation) throws DatasetManagementException {
    clientCache.getUnchecked(moduleId.getParent())
      .addModule(moduleId.getEntityName(), DatasetModules.getDatasetModuleClass(module).getName(), jarLocation);
  }

  @Override
  public void deleteModule(DatasetModuleId moduleId) throws DatasetManagementException {
    clientCache.getUnchecked(moduleId.getParent()).deleteModule(moduleId.getEntityName());
  }

  @Override
  public void deleteAllModules(NamespaceId namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).deleteModules();
  }

  @Override
  public void addInstance(String datasetType, DatasetId datasetInstanceId, DatasetProperties props,
                          @Nullable KerberosPrincipalId ownerPrincipal)
    throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getParent())
      .addInstance(datasetInstanceId.getEntityName(), datasetType, props, ownerPrincipal);
  }

  @Override
  public void updateInstance(DatasetId datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getParent())
      .updateInstance(datasetInstanceId.getEntityName(), props);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(NamespaceId namespaceId)
    throws DatasetManagementException {
    return callWithRetries(() -> clientCache.getUnchecked(namespaceId).getAllInstances());
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(NamespaceId namespaceId, Map<String, String> properties)
    throws DatasetManagementException {
    return callWithRetries(() -> clientCache.getUnchecked(namespaceId).getInstances(properties));
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(DatasetId datasetInstanceId) throws DatasetManagementException {
    DatasetMeta meta = callWithRetries(() -> clientCache.getUnchecked(datasetInstanceId.getParent())
      .getInstance(datasetInstanceId.getEntityName()));
    return meta == null ? null : meta.getSpec();
  }

  @Override
  public boolean hasInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    return callWithRetries(() -> clientCache.getUnchecked(datasetInstanceId.getParent())
      .getInstance(datasetInstanceId.getEntityName()) != null);
  }

  @Override
  public boolean hasType(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return callWithRetries(() -> clientCache.getUnchecked(datasetTypeId.getParent())
      .getType(datasetTypeId.getEntityName()) != null);
  }

  @Override
  public DatasetTypeMeta getTypeInfo(DatasetTypeId datasetTypeId) throws DatasetManagementException {
    return callWithRetries(() -> clientCache.getUnchecked(datasetTypeId.getParent())
      .getType(datasetTypeId.getEntityName()));
  }

  @Override
  public void truncateInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getParent()).truncateInstance(datasetInstanceId.getEntityName());
  }

  @Override
  public void deleteInstance(DatasetId datasetInstanceId) throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getParent()).deleteInstance(datasetInstanceId.getEntityName());
  }

  @Override
  public void deleteAllInstances(NamespaceId namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).deleteInstances();
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId, ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    return getAdmin(datasetInstanceId, classLoader, new ConstantClassLoaderProvider(classLoader));
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(DatasetId datasetInstanceId,
                                             @Nullable ClassLoader parentClassLoader,
                                             DatasetClassLoaderProvider classLoaderProvider)
    throws DatasetManagementException, IOException {
    DatasetMeta instanceInfo = callWithRetries(() -> clientCache.getUnchecked(datasetInstanceId.getParent())
      .getInstance(datasetInstanceId.getEntityName()));
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = getType(instanceInfo.getType(), parentClassLoader, classLoaderProvider);
    return (T) type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespace()), instanceInfo.getSpec());
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(DatasetId id, Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader,
                                          DatasetClassLoaderProvider classLoaderProvider,
                                          @Nullable Iterable<? extends EntityId> owners, AccessType accessType)
    throws DatasetManagementException, IOException {

    DatasetMeta datasetMeta = callWithRetries(() -> clientCache.getUnchecked(id.getParent())
      .getInstance(id.getEntityName()));
    if (datasetMeta == null) {
      return null;
    }

    DatasetType type = getType(datasetMeta.getType(), classLoader, classLoaderProvider);
    return (T) type.getDataset(DatasetContext.from(id.getNamespace()), datasetMeta.getSpec(), arguments);
  }

  @Override
  public void writeLineage(DatasetId datasetInstanceId, AccessType accessType) {
    // no-op. The RemoteDatasetFramework doesn't need to do anything. The lineage should be recorded before this point.
  }

  private Location createDeploymentJar(Class<?> clz) throws IOException {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    tempDir.mkdirs();
    File tempFile = File.createTempFile(clz.getName(), ".jar", tempDir);
    try {
      // Create a bundle jar in a temp location
      ClassLoader remembered = ClassLoaders.setContextClassLoader(clz.getClassLoader());
      try {
        ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("io.cdap.cdap.api",
                                                                             "org.apache.hadoop",
                                                                             "org.apache.hbase",
                                                                             "org.apache.hive"));
        bundler.createBundle(Locations.toLocation(tempFile), clz);
      } finally {
        ClassLoaders.setContextClassLoader(remembered);
      }

      // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
      // by the ApplicationBundler inside Twill.
      File destination = File.createTempFile(clz.getName(), ".jar", tempDir);
      try (
        JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(destination));
        JarInputStream jarInput = new JarInputStream(new FileInputStream(tempFile))
      ) {
        Set<String> seen = Sets.newHashSet();
        JarEntry jarEntry = jarInput.getNextJarEntry();
        while (jarEntry != null) {
          boolean isDir = jarEntry.isDirectory();
          String entryName = jarEntry.getName();
          if (!entryName.equals("classes/")) {
            if (entryName.startsWith("classes/")) {
              jarEntry = new JarEntry(entryName.substring("classes/".length()));
            } else {
              jarEntry = new JarEntry(entryName);
            }
            if (seen.add(jarEntry.getName())) {
              jarOutput.putNextEntry(jarEntry);

              if (!isDir) {
                ByteStreams.copy(jarInput, jarOutput);
              }
            }
          }

          jarEntry = jarInput.getNextJarEntry();
        }

        return Locations.toLocation(destination);
      }
    } finally {
      tempFile.delete();
    }
  }

  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service

  /**
   * Return an instance of the {@link DatasetType} corresponding to given dataset modules. Uses the given
   * classloader as a parent for all dataset modules, and the given classloader provider to get classloaders for
   * each dataset module in given the dataset type meta. Order of dataset modules in the given
   * {@link DatasetTypeMeta} is important. The classloader for the first dataset module is used as the parent of
   * the second dataset module and so on until the last dataset module. The classloader for the last dataset module
   * is then used as the classloader for the returned {@link DatasetType}.
   *
   * @param implementationInfo the dataset type metadata to instantiate the type from
   * @param classLoader the parent classloader to use for dataset modules
   * @param classLoaderProvider the classloader provider to get classloaders for each dataset module
   * @param <T> the type of DatasetType
   * @return an instance of the DatasetType
   */
  public <T extends DatasetType> T getDatasetType(DatasetTypeMeta implementationInfo,
                                                  ClassLoader classLoader,
                                                  DatasetClassLoaderProvider classLoaderProvider) {

    return getType(implementationInfo, classLoader, classLoaderProvider);
  }

  /**
   * Return an instance of the {@link DatasetType} corresponding to given dataset modules. Uses the given
   * classloader as a parent for all dataset modules, and the given classloader provider to get classloaders for
   * each dataset module in given the dataset type meta. Order of dataset modules in the given
   * {@link DatasetTypeMeta} is important. The classloader for the first dataset module is used as the parent of
   * the second dataset module and so on until the last dataset module. The classloader for the last dataset module
   * is then used as the classloader for the returned {@link DatasetType}.
   *
   * @param datasetTypeMeta the dataset type metadata to instantiate the type from
   * @param classLoader the parent classloader to use for dataset modules
   * @param classLoaderProvider the classloader provider to get classloaders for each dataset module
   * @param <T> the type of DatasetType
   * @return an instance of the DatasetType
   */
  private <T extends DatasetType> T getType(DatasetTypeMeta datasetTypeMeta,
                                            @Nullable ClassLoader classLoader,
                                            DatasetClassLoaderProvider classLoaderProvider) {

    if (classLoader == null) {
      classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader());
    }

    DatasetDefinitionRegistry registry = registryFactory.create();
    for (DatasetModuleMeta moduleMeta : datasetTypeMeta.getModules()) {
      // adding dataset module jar to classloader
      try {
        classLoader = classLoaderProvider.get(moduleMeta, classLoader);
      } catch (IOException e) {
        LOG.error("Was not able to init classloader for module {} while trying to load type {}",
                  moduleMeta, datasetTypeMeta, e);
        throw Throwables.propagate(e);
      }

      try {
        DatasetDefinitionRegistries.register(moduleMeta.getClassName(), classLoader, registry);
      } catch (Exception e) {
        LOG.error("Was not able to load dataset module class {} while trying to load type {}",
                  moduleMeta.getClassName(), datasetTypeMeta, e);
        throw Throwables.propagate(e);
      }
    }

    // contract of DatasetTypeMeta is that the last module returned by getModules() is the one
    // that announces the dataset's type. The classloader for the returned DatasetType must be the classloader
    // for that last module.
    return (T) new DatasetType(registry.get(datasetTypeMeta.getName()), classLoader);
  }

  /**
   * helper method to retry with proper strategy and predicate
   */
  private <V, T extends Throwable> V callWithRetries(Retries.Callable<V, T> callable) throws T {
    return Retries.<V, T>callWithRetries(callable, retryStrategy, RETRYABLE_PREDICATE);
  }
}
