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

import java.io.Closeable;
import java.io.IOException;

/**
 * Gets {@link ClassLoader ClassLoaders} for different {@link DatasetModuleMeta}.
 */
public interface DatasetClassLoaderProvider extends Closeable {

  /**
   * Get the classloader for a specific dataset module.
   *
   * @param moduleMeta the metadata for the dataset module to get a classloader for
   * @return classloader for the given dataset module
   * @throws IOException
   */
  ClassLoader get(DatasetModuleMeta moduleMeta) throws IOException;

  /**
   * Get the parent classloader for module classloaders
   *
   * @return the parent classloader for module classloaders
   * @throws IOException
   */
  ClassLoader getParent();
}
