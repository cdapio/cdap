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

import co.cask.cdap.proto.DatasetModuleMeta;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} without unpacking the moduleMeta jar,
 * since the jar file is already present in the local filesystem.
 */
public class LocalDatasetTypeClassLoaderFactory implements DatasetTypeClassLoaderFactory {

  @Override
  public ClassLoader create(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException {
    if (moduleMeta.getJarLocation() == null) {
      return parentClassLoader;
    }

    return new URLClassLoader(new URL[]{moduleMeta.getJarLocation().toURL()}, parentClassLoader);
  }
}
