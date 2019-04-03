/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.data2.datafabric.dataset.type.ConstantClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetClassLoaderProvider;
import co.cask.cdap.data2.datafabric.dataset.type.DirectoryClassLoaderProvider;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

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
  private final Iterable<? extends EntityId> owners;
  private final ClassLoader parentClassLoader;

  /**
   * Creates a new instance with the given {@link DatasetFramework}. It will always use the context classloader
   * for loading dataset classes.
   */
  public SystemDatasetInstantiator(DatasetFramework datasetFramework) {
    this(datasetFramework, null, null);
  }

  public SystemDatasetInstantiator(DatasetFramework datasetFramework,
                                   @Nullable ClassLoader classLoader,
                                   @Nullable Iterable<? extends EntityId> owners) {
    this(datasetFramework, classLoader, new ConstantClassLoaderProvider(classLoader), owners);
  }

  SystemDatasetInstantiator(DatasetFramework datasetFramework,
                            @Nullable ClassLoader parentClassLoader,
                            DatasetClassLoaderProvider classLoaderProvider,
                            @Nullable Iterable<? extends EntityId> owners) {
    this.owners = owners;
    this.classLoaderProvider = classLoaderProvider;
    this.datasetFramework = datasetFramework;
    this.parentClassLoader = parentClassLoader == null ?
      Objects.firstNonNull(Thread.currentThread().getContextClassLoader(), getClass().getClassLoader()) :
      parentClassLoader;
  }

  @Nullable
  public <T extends DatasetAdmin> T getDatasetAdmin(DatasetId datasetId) throws DatasetManagementException,
    IOException {
    return datasetFramework.getAdmin(datasetId, parentClassLoader, classLoaderProvider);
  }

  public <T extends Dataset> T getDataset(DatasetId datasetId)
    throws DatasetInstantiationException {
    return getDataset(datasetId, DatasetDefinition.NO_ARGUMENTS);
  }

  public <T extends Dataset> T getDataset(DatasetId datasetId, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return getDataset(datasetId, arguments, AccessType.UNKNOWN);
  }

  @SuppressWarnings("unchecked")
  public <T extends Dataset> T getDataset(DatasetId datasetId, Map<String, String> arguments,
                                          AccessType accessType) throws DatasetInstantiationException {

    try {
      T dataset = datasetFramework.getDataset(datasetId, arguments, parentClassLoader, classLoaderProvider, owners,
                                              accessType);
      if (dataset == null) {
        throw new DatasetInstantiationException("Trying to access dataset that does not exist: " + datasetId);
      }
      return dataset;
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, ServiceUnavailableException.class);
      throw new DatasetInstantiationException("Failed to access dataset: " + datasetId, e);
    }
  }

  public void writeLineage(DatasetId datasetId, AccessType accessType) {
    datasetFramework.writeLineage(datasetId, accessType);
  }

  @Override
  public void close() throws IOException {
    classLoaderProvider.close();
  }
}
