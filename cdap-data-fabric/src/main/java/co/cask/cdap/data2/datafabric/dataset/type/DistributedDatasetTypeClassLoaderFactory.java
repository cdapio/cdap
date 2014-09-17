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

package co.cask.cdap.data2.datafabric.dataset.type;

import co.cask.cdap.common.lang.ApiResourceListHolder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.proto.DatasetModuleMeta;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} by unpacking the moduleMeta jar,
 * since the moduleMeta jar may not be present in the local filesystem.
 */
public class DistributedDatasetTypeClassLoaderFactory implements DatasetTypeClassLoaderFactory {

  private final LocationFactory locationFactory;

  @Inject
  public DistributedDatasetTypeClassLoaderFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public ClassLoader create(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException {
    if (moduleMeta.getJarLocation() == null) {
      return parentClassLoader;
    }

    // creating tempDir is fine since it will be created inside a YARN container, so it will be cleaned up
    File tempDir = Files.createTempDir();
    BundleJarUtil.unpackProgramJar(locationFactory.create(moduleMeta.getJarLocation()), tempDir);
    return ClassLoaders.newProgramClassLoader(tempDir, ApiResourceListHolder.getResourceList(), parentClassLoader);
  }
}
