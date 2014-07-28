/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.datafabric.dataset.type;

import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.lang.ClassLoaders;
import com.continuuity.common.lang.jar.JarClassLoader;
import com.continuuity.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.module.lib.DatasetModules;
import com.continuuity.proto.DatasetModuleMeta;
import com.continuuity.proto.DatasetTypeMeta;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;

/**
 * Loads {@link DatasetDefinition} using its metadata info
 */
public class DatasetDefinitionLoader {
  private final LocationFactory locationFactory;

  /**
   * Creates instance of {@link DatasetDefinitionLoader}
   * @param locationFactory instance of {@link LocationFactory} used to access dataset modules jars
   */
  public DatasetDefinitionLoader(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  /**
   * Same as {@link #load(DatasetTypeMeta, DatasetDefinitionRegistry)} but uses empty registry to star with`.
   */
  public <T extends DatasetDefinition> T load(DatasetTypeMeta meta) throws IOException {
    return load(meta, new InMemoryDatasetDefinitionRegistry());
  }

  /**
   * Loads {@link DatasetDefinition} using {@link DatasetTypeMeta} info. It will use given
   * {@link DatasetDefinitionRegistry} to load all required modules and types. If registry is missing some of them,
   * it will load respective jars and add them to the registry (thus, modifying the given registry).
   * @param meta info of type to load
   * @param registry registry to use for loading
   * @param <T> type of the definition
   * @return instance of {@link DatasetDefinition}
   * @throws IOException
   */
  public <T extends DatasetDefinition> T load(DatasetTypeMeta meta, DatasetDefinitionRegistry registry)
    throws IOException {

    ClassLoader classLoader = DatasetDefinitionLoader.class.getClassLoader();
    List<DatasetModuleMeta> modulesToLoad = meta.getModules();
    for (DatasetModuleMeta moduleMeta : modulesToLoad) {
      // for default "system" modules it can be null, see getJarLocation() javadoc
      if (moduleMeta.getJarLocation() != null) {
        classLoader = new JarClassLoader(locationFactory.create(moduleMeta.getJarLocation()), classLoader);
      }
      DatasetModule module;
      try {
        Class<?> moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), classLoader, this);
        module = DatasetModules.getDatasetModule(moduleClass);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
      module.register(registry);
    }

    return registry.get(meta.getName());
  }
}
