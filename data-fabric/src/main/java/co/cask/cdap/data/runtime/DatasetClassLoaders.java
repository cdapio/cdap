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

package co.cask.cdap.data.runtime;

import co.cask.cdap.common.lang.ApiResourceListHolder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Helps to share a functionality to create ClassLoader with Dataset Types,  provided {@link java.lang.ClassLoader} ,
 * {@link co.cask.cdap.proto.DatasetTypeMeta} and {@link org.apache.twill.filesystem.LocationFactory}
 */
public class DatasetClassLoaders {

  /**
   * Returns {@link java.lang.ClassLoader }provided a parent {@link java.lang.ClassLoader} ,
   * {@link co.cask.cdap.proto.DatasetTypeMeta} and {@link org.apache.twill.filesystem.LocationFactory}
   * @param cl
   * @param typeMeta
   * @param locationFactory
   * @return {@link java.lang.ClassLoader}
   */
  public static DatasetClassLoaderUtil createDatasetClassLoaderFromType(ClassLoader cl, DatasetTypeMeta typeMeta,
                                                             LocationFactory locationFactory) {
    try {
      List<DatasetModuleMeta> modulesToLoad = typeMeta.getModules();
      List<File> datasetFiles = Lists.newArrayList();
      for (DatasetModuleMeta module : modulesToLoad) {
        if (module.getJarLocation() != null) {
          File tempDir = Files.createTempDir();
          BundleJarUtil.unpackProgramJar(locationFactory.create(module.getJarLocation()), tempDir);
          datasetFiles.add(tempDir);
        }
      }
      if (!datasetFiles.isEmpty()) {
        return new DatasetClassLoaderUtil(ClassLoaders.newDatasetClassLoader
          (datasetFiles, ApiResourceListHolder.getResourceList(), cl), datasetFiles);
      } else {
        return new DatasetClassLoaderUtil(cl, datasetFiles);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
