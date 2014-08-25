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
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.util.List;

/**
 * Helps to share a functionality to create ClassLoader with Dataset Types,  provided {@link java.lang.ClassLoader} ,
 * {@link co.cask.cdap.proto.DatasetTypeMeta} and {@link org.apache.twill.filesystem.LocationFactory}
 */
public class DatasetClassLoaderFactory {

  /**
   * Returns {@link java.lang.ClassLoader }provided a parent {@link java.lang.ClassLoader} ,
   * {@link co.cask.cdap.proto.DatasetTypeMeta} and {@link org.apache.twill.filesystem.LocationFactory}
   * @param cl
   * @param typeMeta
   * @param locationFactory
   * @return {@link java.lang.ClassLoader}
   */
  public static ClassLoader createDatasetClassLoaderFromType(ClassLoader cl, DatasetTypeMeta typeMeta,
                                                             LocationFactory locationFactory) {
    try {
      List<DatasetModuleMeta> modulesToLoad = typeMeta.getModules();
      List<Location> datasetJars = Lists.newArrayList();
      for (DatasetModuleMeta module : modulesToLoad) {
        if (module.getJarLocation() != null) {
          datasetJars.add(locationFactory.create(module.getJarLocation()));
        }
      }
      if (!datasetJars.isEmpty()) {
        return ClassLoaders.newDatasetClassLoader(datasetJars, ApiResourceListHolder.getResourceList(), cl);
      } else {
        return cl;
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

}
