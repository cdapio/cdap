/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.lang.ApiResourceListHolder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.InMemoryDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Loads {@link DatasetDefinition} using its metadata info
 */
class DatasetDefinitionLoader {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetDefinitionLoader.class);

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
    File unpackedLocation = Files.createTempDir();
    int index = 0;
    try {
      for (DatasetModuleMeta moduleMeta : modulesToLoad) {
        File temp = new File(unpackedLocation, String.valueOf(index++));
        temp.mkdir();
        // for default "system" modules it can be null, see getJarLocation() javadoc
        if (moduleMeta.getJarLocation() != null) {
          BundleJarUtil.unpackProgramJar(locationFactory.create(moduleMeta.getJarLocation()), temp);
          classLoader = ClassLoaders.newProgramClassLoader(temp, ApiResourceListHolder.getResourceList(),
                                                           this.getClass().getClassLoader());
        }
        Class<?> moduleClass = ClassLoaders.loadClass(moduleMeta.getClassName(), classLoader, this);
        DatasetModule module = DatasetModules.getDatasetModule(moduleClass);
        module.register(registry);
      }
    } catch (Exception e) {
      LOG.warn("Exception while loading DatasetDefinition for DatasetTypeMeta : {}", meta.getName());
    } finally {
      try {
        DirUtils.deleteDirectoryContents(unpackedLocation);
      } catch (IOException e) {
        LOG.warn("Failed to delete directory {}", unpackedLocation, e);
      }
    }
    return registry.get(meta.getName());
  }
}
