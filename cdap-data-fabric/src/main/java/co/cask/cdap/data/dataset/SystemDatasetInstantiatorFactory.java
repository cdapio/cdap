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

package co.cask.cdap.data.dataset;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.datafabric.dataset.type.DirectoryClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

import javax.annotation.Nullable;

/**
 * Creates {@link SystemDatasetInstantiator} instances for use in system contexts. This is used instead of directly
 * creating a {@link SystemDatasetInstantiator} to ensure that the same temporary base directory is used,
 * and so that the factory can be injected directly instead of injecting a LocationFactory, DatasetFramework,
 * and CConfiguration object everywhere it is used.
 */
public class SystemDatasetInstantiatorFactory {
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final CConfiguration cConf;

  @Inject
  private SystemDatasetInstantiatorFactory(LocationFactory locationFactory,
                                           DatasetFramework datasetFramework,
                                           CConfiguration cConf) {
    this.locationFactory = locationFactory;
    this.datasetFramework = datasetFramework;
    this.cConf = cConf;
  }

  public SystemDatasetInstantiator create() {
    return create(null);
  }

  public SystemDatasetInstantiator create(@Nullable ClassLoader parentClassLoader) {
    return new SystemDatasetInstantiator(datasetFramework,
      new DirectoryClassLoaderProvider(cConf, parentClassLoader, locationFactory),
      null);
  }

}
