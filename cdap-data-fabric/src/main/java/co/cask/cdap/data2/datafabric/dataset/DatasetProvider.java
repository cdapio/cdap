/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides {@link Dataset} instances directly using {@link DatasetSpecification}.
 */
@SuppressWarnings("unchecked")
public class DatasetProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetProvider.class);
  private final DatasetDefinitionRegistryFactory registryFactory;

  @Inject
  public DatasetProvider(DatasetDefinitionRegistryFactory registryFactory) {
    this.registryFactory = registryFactory;
  }

  public <T extends Dataset> T get(
    DatasetMetaProvider metaProvider,
    Id.DatasetInstance instance,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws Exception {

    return get(instance.getNamespace(), metaProvider.getType(instance), metaProvider.getSpec(instance),
               null, classLoader, arguments);
  }

  public <T extends Dataset> T get(
    Id.Namespace namespace,
    DatasetTypeMeta typeMeta,
    DatasetSpecification spec,
    @Nullable DatasetClassLoaderProvider classLoaderProvider,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws IOException {

    classLoaderProvider = classLoaderProvider == null ?
      new ConstantClassLoaderProvider(classLoader) : classLoaderProvider;
    DatasetType type = getType(typeMeta, classLoader, classLoaderProvider);
    return (T) type.getDataset(DatasetContext.from(namespace.getId()), spec, arguments);
  }

  public <T extends Dataset> T get(Id.Namespace namespace,
                                   DatasetTypeMeta typeMeta,
                                   DatasetSpecification spec) throws IOException {
    return get(namespace, typeMeta, spec, null, null, null);
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
  public <T extends DatasetType> T getType(
    DatasetTypeMeta implementationInfo,
    ClassLoader classLoader,
    DatasetClassLoaderProvider classLoaderProvider) {

    if (classLoader == null) {
      classLoader = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader());
    }

    DatasetDefinitionRegistry registry = registryFactory.create();
    List<DatasetModuleMeta> modulesToLoad = implementationInfo.getModules();
    for (DatasetModuleMeta moduleMeta : modulesToLoad) {
      // adding dataset module jar to classloader
      try {
        classLoader = classLoaderProvider.get(moduleMeta, classLoader);
      } catch (IOException e) {
        LOG.error("Was not able to init classloader for module {} while trying to load type {}",
                  moduleMeta, implementationInfo, e);
        throw Throwables.propagate(e);
      }

      Class<?> moduleClass;

      // try program class loader then cdap class loader
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

    // contract of DatasetTypeMeta is that the last module returned by getModules() is the one
    // that announces the dataset's type. The classloader for the returned DatasetType must be the classloader
    // for that last module.
    return (T) new DatasetType(registry.get(implementationInfo.getName()), classLoader);
  }
}
