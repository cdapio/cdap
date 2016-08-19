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

package co.cask.cdap.data2.datafabric.dataset.instance;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.data2.datafabric.dataset.service.mds.DatasetInstanceMDS;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.data2.transaction.TransactionSystemClientService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.TransactionExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Manages dataset instances metadata
 */
public class DatasetInstanceManager {

  private final TransactionExecutorFactory txExecutorFactory;
  private final DynamicDatasetCache datasetCache;

  @Inject
  public DatasetInstanceManager(TransactionSystemClientService txClientService,
                                TransactionExecutorFactory txExecutorFactory,
                                @Named("datasetMDS") DatasetFramework datasetFramework) {
    this.txExecutorFactory = txExecutorFactory;

    Map<String, String> emptyArgs = Collections.emptyMap();
    this.datasetCache = new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework, null, null),
                                                    txClientService, NamespaceId.SYSTEM, emptyArgs, null,
                                                    ImmutableMap.of(
                                                      DatasetMetaTableUtil.INSTANCE_TABLE_NAME, emptyArgs
                                                    ));
  }

  /**
   * Adds dataset instance metadata
   * @param namespaceId the {@link Id.Namespace} to add the dataset instance to
   * @param spec {@link DatasetSpecification} of the dataset instance to be added
   */
  public void add(final Id.Namespace namespaceId, final DatasetSpecification spec) {
    final DatasetInstanceMDS instanceMDS = datasetCache.getDataset(DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
    txExecutorFactory.createExecutor(datasetCache).executeUnchecked(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        instanceMDS.write(namespaceId, spec);
      }
    });
  }

  /**
   * @param datasetInstanceId {@link Id.DatasetInstance} of the dataset instance
   * @return dataset instance's {@link DatasetSpecification}
   */
  @Nullable
  public DatasetSpecification get(final Id.DatasetInstance datasetInstanceId) {
    final DatasetInstanceMDS instanceMDS = datasetCache.getDataset(DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
    return txExecutorFactory.createExecutor(datasetCache).executeUnchecked(new Callable<DatasetSpecification>() {
      @Override
      public DatasetSpecification call() throws Exception {
        return instanceMDS.get(datasetInstanceId);
      }
    });
  }

  /**
   * @param namespaceId {@link Id.Namespace} for which dataset instances are required
   * @return collection of {@link DatasetSpecification} of all dataset instances in the given namespace
   */
  public Collection<DatasetSpecification> getAll(final Id.Namespace namespaceId) {
    final DatasetInstanceMDS instanceMDS = datasetCache.getDataset(DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
    return txExecutorFactory.createExecutor(datasetCache)
      .executeUnchecked(new Callable<Collection<DatasetSpecification>>() {
        @Override
        public Collection<DatasetSpecification> call() throws Exception {
          return instanceMDS.getAll(namespaceId);
        }
      });
  }

  /**
   * Deletes dataset instance
   * @param datasetInstanceId {@link Id.DatasetInstance} of the instance to delete
   * @return true if deletion succeeded, false otherwise
   */
  public boolean delete(final Id.DatasetInstance datasetInstanceId) {
    final DatasetInstanceMDS instanceMDS = datasetCache.getDataset(DatasetMetaTableUtil.INSTANCE_TABLE_NAME);
    return txExecutorFactory.createExecutor(datasetCache).executeUnchecked(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return instanceMDS.delete(datasetInstanceId);
      }
    });
  }
}
