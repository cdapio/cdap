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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeClassLoaderFactory;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides {@link Dataset} instances using methods implemented by subclasses.
 * Used by {@link RemoteDatasetFramework} and
 * {@link co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceHandler}.
 * Use this when you want to control how dataset instances are created. For example, when
 * you want to obtain a {@link Dataset} instance without having to make remote calls.
 */
public abstract class AbstractDatasetProvider implements DatasetProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDatasetProvider.class);
  private final DatasetDefinitionRegistryFactory registryFactory;
  private final DatasetTypeClassLoaderFactory typeLoader;

  protected AbstractDatasetProvider(DatasetDefinitionRegistryFactory registryFactory,
                                    DatasetTypeClassLoaderFactory typeLoader) {
    this.registryFactory = registryFactory;
    this.typeLoader = typeLoader;
  }

  /**
   * Gets the {@link DatasetMeta} for a dataset.
   *
   * @param instance the dataset
   * @return the {@link DatasetMeta}
   */
  public abstract DatasetMeta getMeta(Id.DatasetInstance instance) throws Exception;

  /**
   * Creates the dataset if it doesn't already exist.
   *
   * @param instance the dataset
   * @param type the type of dataset to create
   * @param creationProps creation properties
   */
  public abstract void createIfNotExists(Id.DatasetInstance instance, String type,
                                         DatasetProperties creationProps) throws Exception;

  @Override
  public <T extends Dataset> T getOrCreate(
    Id.DatasetInstance instance, String type,
    DatasetProperties creationProps,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws Exception {

    try {
      T result = get(instance, classLoader, arguments);
      if (result != null) {
        return result;
      }
    } catch (NotFoundException e) {
      // fall-through to create
    }

    createIfNotExists(instance, type, creationProps);
    return get(instance, classLoader, arguments);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Dataset> T get(
    Id.DatasetInstance instance,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws Exception {

    DatasetMeta meta = getMeta(instance);
    DatasetType type = getDatasetType(meta.getType(), classLoader);
    return (T) type.getDataset(
      DatasetContext.from(instance.getNamespaceId()), meta.getSpec(), arguments);
  }

  @SuppressWarnings("unchecked")
  public <T extends Dataset> T get(
    Id.DatasetInstance instance,
    DatasetTypeMeta typeMeta, DatasetSpecification spec,
    @Nullable ClassLoader classLoader,
    @Nullable Map<String, String> arguments) throws IOException {

    DatasetType type = getDatasetType(typeMeta, classLoader);
    return (T) type.getDataset(DatasetContext.from(instance.getNamespaceId()), spec, arguments);
  }


  // can be used directly if DatasetTypeMeta is known, like in create dataset by dataset ops executor service
  public <T extends DatasetType> T getDatasetType(
    DatasetTypeMeta implementationInfo,
    ClassLoader classLoader) {

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

    return (T) new DatasetType(registry.get(implementationInfo.getName()), classLoader);
  }
}
