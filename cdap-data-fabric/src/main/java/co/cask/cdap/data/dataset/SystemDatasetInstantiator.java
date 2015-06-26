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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DirectoryClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.Id;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The data set instantiator creates instances of data sets at runtime. It is mostly a wrapper around
 * {@link DatasetFramework}, except it is closeable so that any resources created by dataset classloaders
 * can be properly cleaned up. For example, a {@link DirectoryClassLoaderProvider} will fetch dataset jars
 * and unpack them in a local directory, which must be cleaned up after the dataset is no longer needed.
 */
public class SystemDatasetInstantiator implements Closeable {

  private final DatasetFramework datasetFramework;
  // provides classloaders to use for different dataset modules
  private final DatasetClassLoaderProvider classLoaderProvider;
  private final Iterable<? extends Id> owners;

  SystemDatasetInstantiator(DatasetFramework datasetFramework,
                            DatasetClassLoaderProvider classLoaderProvider,
                            @Nullable Iterable<? extends Id> owners) {
    this.owners = owners;
    this.classLoaderProvider = classLoaderProvider;
    this.datasetFramework = datasetFramework;
  }

  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetId)
    throws DatasetInstantiationException {
    return getDataset(datasetId, DatasetDefinition.NO_ARGUMENTS);
  }

  @SuppressWarnings("unchecked")
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetId, Map<String, String> arguments)
    throws DatasetInstantiationException {

    T dataset;
    try {
      if (!datasetFramework.hasInstance(datasetId)) {
        throw new DatasetInstantiationException("Trying to access dataset that does not exist: " + datasetId);
      }

      dataset = datasetFramework.getDataset(datasetId, arguments, classLoaderProvider, owners);
      if (dataset == null) {
        throw new DatasetInstantiationException("Failed to access dataset: " + datasetId);
      }

    } catch (Exception e) {
      throw new DatasetInstantiationException("Failed to access dataset: " + datasetId, e);
    }

    return dataset;
  }

  @Override
  public void close() throws IOException {
    classLoaderProvider.close();
  }
}
