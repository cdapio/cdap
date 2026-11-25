/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package io.cdap.cdap.data.dataset;

import com.google.inject.Inject;
import io.cdap.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import javax.annotation.Nullable;

/**
 * Creates {@link SystemDatasetInstantiator} instances for use in system contexts. This is used
 * instead of directly creating a {@link SystemDatasetInstantiator} so that the factory can be
 * injected directly wherever it is used.
 */
public class SystemDatasetInstantiatorFactory {

  private final DatasetFramework datasetFramework;

  @Inject
  public SystemDatasetInstantiatorFactory(DatasetFramework datasetFramework) {
    this.datasetFramework = datasetFramework;
  }

  /**
   * Create a {@link SystemDatasetInstantiator} using the system classloader as the parent
   * classloader.
   *
   * @return a {@link SystemDatasetInstantiator} using the system classloader as the parent
   *     classloader
   */
  public SystemDatasetInstantiator create() {
    return create(null);
  }

  /**
   * Create a {@link SystemDatasetInstantiator} that uses the given classloader as the parent when
   * instantiating datasets.
   *
   * @param parentClassLoader the parent classloader to use when instantiating datasets. If
   *     null, the system classloader will be used
   * @return a {@link SystemDatasetInstantiator} using the given classloader as the parent
   *     classloader
   */
  public SystemDatasetInstantiator create(@Nullable ClassLoader parentClassLoader) {
    return new SystemDatasetInstantiator(datasetFramework, parentClassLoader,
        new ConstantClassLoaderProvider(parentClassLoader), null);
  }

}
