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

package co.cask.cdap.data2.datafabric.dataset;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ApplicationBundler;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.InstanceConflictException;
import co.cask.cdap.data2.dataset2.SingleTypeModule;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

/**
 * {@link co.cask.cdap.data2.dataset2.DatasetFramework} implementation that talks to DatasetFramework Service
 */
@SuppressWarnings("unchecked")
public class RemoteDatasetFramework implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDatasetFramework.class);

  private final LoadingCache<Id.Namespace, DatasetServiceClient> clientCache;
  private final AbstractDatasetProvider instances;

  @Inject
  public RemoteDatasetFramework(final DiscoveryServiceClient discoveryClient,
                                DatasetDefinitionRegistryFactory registryFactory,
                                DatasetTypeClassLoaderFactory typeLoader) {
    this.clientCache = CacheBuilder.newBuilder().build(new CacheLoader<Id.Namespace, DatasetServiceClient>() {
      @Override
      public DatasetServiceClient load(Id.Namespace namespace) throws Exception {
        return new DatasetServiceClient(discoveryClient, namespace);
      }
    });
    this.instances = new AbstractDatasetProvider(registryFactory, typeLoader) {
      @Override
      public DatasetMeta getMeta(Id.DatasetInstance instance) throws Exception {
        return RemoteDatasetFramework.this.clientCache.getUnchecked(instance.getNamespace())
          .getInstance(instance.getId());
      }

      @Override
      public void createIfNotExists(Id.DatasetInstance instance, String type,
                                    DatasetProperties creationProps) throws Exception {
        try {
          RemoteDatasetFramework.this.addInstance(type, instance, creationProps);
        } catch (InstanceConflictException e) {
          // ignore, since this indicates the dataset already exists
        }
      }
    };
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module)
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

    addModule(moduleId, typeClass);
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    clientCache.getUnchecked(moduleId.getNamespace()).deleteModule(moduleId.getId());
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).deleteModules();
  }

  @Override
  public void addInstance(String datasetType, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .addInstance(datasetInstanceId.getId(), datasetType, props);
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .updateInstance(datasetInstanceId.getId(), props);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId)
    throws DatasetManagementException {
    return clientCache.getUnchecked(namespaceId).getAllInstances();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    DatasetMeta meta = clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .getInstance(datasetInstanceId.getId());
    return meta == null ? null : meta.getSpec();
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return clientCache.getUnchecked(datasetInstanceId.getNamespace()).getInstance(datasetInstanceId.getId()) != null;
  }

  @Override
  public boolean hasSystemType(String typeName) throws DatasetManagementException {
    return hasType(Id.DatasetType.from(Constants.SYSTEM_NAMESPACE, typeName));
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) throws DatasetManagementException {
    return clientCache.getUnchecked(datasetTypeId.getNamespace()).getType(datasetTypeId.getTypeName()) != null;
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    clientCache.getUnchecked(datasetInstanceId.getNamespace()).deleteInstance(datasetInstanceId.getId());
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    // delete all one by one
    for (DatasetSpecificationSummary metaSummary : getInstances(namespaceId)) {
      Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespaceId, metaSummary.getName());
      deleteInstance(datasetInstanceId);
    }
  }

  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    DatasetMeta instanceInfo = clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .getInstance(datasetInstanceId.getId());
    if (instanceInfo == null) {
      return null;
    }

    DatasetType type = instances.getDatasetType(instanceInfo.getType(), classLoader);
    return (T) type.getAdmin(DatasetContext.from(datasetInstanceId.getNamespaceId()), instanceInfo.getSpec());
  }

  @Override
  public <T extends Dataset> T getDataset(
    Id.DatasetInstance datasetInstanceId, Map<String, String> arguments,
    @Nullable ClassLoader classLoader,
    @Nullable Iterable<? extends Id> owners) throws DatasetManagementException, IOException {

    DatasetMeta instanceInfo = clientCache.getUnchecked(datasetInstanceId.getNamespace())
      .getInstance(datasetInstanceId.getId(), owners);
    if (instanceInfo == null) {
      return null;
    }

    return instances.get(datasetInstanceId, instanceInfo.getType(), instanceInfo.getSpec(),
                         classLoader, arguments);
  }

  @Override
  public <T extends Dataset> T getDataset(
    Id.DatasetInstance datasetInstanceId, Map<String, String> arguments,
    @Nullable ClassLoader classLoader) throws DatasetManagementException, IOException {

    return getDataset(datasetInstanceId, arguments, classLoader, null);
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).createNamespace();
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    clientCache.getUnchecked(namespaceId).deleteNamespace();
  }

  private void addModule(Id.DatasetModule moduleId, Class<?> typeClass) throws DatasetManagementException {
    try {
      File tempFile = File.createTempFile(typeClass.getName(), ".jar");
      try {
        Location tempJarPath = createDeploymentJar(typeClass, new LocalLocationFactory().create(tempFile.toURI()));
        clientCache.getUnchecked(moduleId.getNamespace()).addModule(moduleId.getId(), typeClass.getName(), tempJarPath);
      } finally {
        tempFile.delete();
      }
    } catch (IOException e) {
      String msg = String.format("Could not create jar for deploying dataset module %s with main class %s",
                                 moduleId, typeClass.getName());
      LOG.error(msg, e);
      throw new DatasetManagementException(msg, e);
    }
  }

  private static Location createDeploymentJar(Class<?> clz, Location destination) throws IOException {
    Location tempBundle = destination.getTempFile(".jar");
    try {
      ClassLoader remembered = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(clz.getClassLoader());
      try {
        ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("co.cask.cdap.api",
                                                                             "org.apache.hadoop",
                                                                             "org.apache.hbase",
                                                                             "org.apache.hive"));
        bundler.createBundle(tempBundle, clz);
      } finally {
        Thread.currentThread().setContextClassLoader(remembered);
      }

      // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
      // by the ApplicationBundler inside Twill.
      JarOutputStream jarOutput = new JarOutputStream(destination.getOutputStream());
      try {
        JarInputStream jarInput = new JarInputStream(tempBundle.getInputStream());
        try {
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
        } finally {
          jarInput.close();
        }

      } finally {
        jarOutput.close();
      }

      return destination;
    } finally {
      tempBundle.delete();
    }
  }

  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service
  public <T extends DatasetType> T getDatasetType(DatasetTypeMeta implementationInfo,
                                                  ClassLoader classLoader) throws DatasetManagementException {

    return instances.getDatasetType(implementationInfo, classLoader);
  }
}
