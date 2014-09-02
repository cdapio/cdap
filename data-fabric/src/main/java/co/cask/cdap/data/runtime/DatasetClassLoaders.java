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

import co.cask.cdap.common.lang.AnnotationListHolder;
import co.cask.cdap.common.lang.ApiResourceListHolder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Helps to share a functionality to create ClassLoader with Dataset Types,  provided {@link java.lang.ClassLoader} ,
 * {@link co.cask.cdap.proto.DatasetTypeMeta} and {@link org.apache.twill.filesystem.LocationFactory}
 */
public class DatasetClassLoaders {

  /**
   * Constructs and Returns {@link ClassLoader } provided a parent {@link ClassLoader},
   * {@link DatasetTypeMeta} and {@link LocationFactory}.
   * This clasloader contains the dataset classes based on the supplied {@link DatasetTypeMeta}
   * @param parentClassLoader
   * @param typeMeta
   * @param locationFactory
   * @return {@link java.lang.ClassLoader}
   */
  public static DatasetClassLoaderUtil createDatasetClassLoaderFromType(ClassLoader parentClassLoader,
                                                                        DatasetTypeMeta typeMeta,
                                                                        LocationFactory locationFactory) {
    try {
      List<DatasetModuleMeta> modulesToLoad = typeMeta.getModules();
      List<File> datasetFiles = Lists.newArrayList();
      Set<URI> newModuleLocation = Sets.newHashSet();

      for (DatasetModuleMeta module : modulesToLoad) {
        if ((module.getJarLocation() != null) && newModuleLocation.add(module.getJarLocation())) {
          File tempDir = Files.createTempDir();
          BundleJarUtil.unpackProgramJar(locationFactory.create(module.getJarLocation()), tempDir);
          datasetFiles.add(tempDir);
        }
      }
      if (!datasetFiles.isEmpty()) {
        return new DatasetClassLoaderUtil(ClassLoaders.newAnnotationFilterClassLoader
          (datasetFiles, ApiResourceListHolder.getResourceList(),
           parentClassLoader, AnnotationListHolder.getAnnotationList()), datasetFiles);
      } else {
        return new DatasetClassLoaderUtil(parentClassLoader, datasetFiles);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
