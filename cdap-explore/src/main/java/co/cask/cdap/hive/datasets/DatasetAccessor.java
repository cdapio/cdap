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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.DatasetNotFoundException;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.context.TxnCodec;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;

/**
 * Instantiates the dataset used during runtime of a Hive query. This means it's used in mappers and reducers,
 * and must use the Hadoop configuration to look up what dataset to instantiate. Should not be closed until the
 * dataset has been closed. Assumes the dataset name and namespace are settings in the configuration.
 * It may seem like this would not work if multiple datasets are used in a single query, but that is not the case.
 * It is not obvious, but dataset name and namespace are added as job properties in DatasetStorageHandler. This tells
 * Hive to add those properties to the Configuration object before passing it in to the methods of an InputFormat
 * or OutputFormat. So even if multiple datasets are used in the same query (a join query for example), dataset name
 * will not get clobbered.
 */
public class DatasetAccessor implements Closeable {
  private final Id.DatasetInstance datasetId;
  private final ContextManager.Context context;
  private final Transaction transaction;
  private final SystemDatasetInstantiator datasetInstantiator;
  private Dataset dataset;

  public DatasetAccessor(Configuration conf) throws IOException {
    String datasetName = conf.get(Constants.Explore.DATASET_NAME);
    String namespace = conf.get(Constants.Explore.DATASET_NAMESPACE);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetName), "dataset name not present in config");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "namespace not present in config");

    this.datasetId = Id.DatasetInstance.from(namespace, datasetName);
    this.context = ContextManager.getContext(conf);
    this.transaction = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
    this.datasetInstantiator = context.createDatasetInstantiator(conf.getClassLoader());
  }

  public void initialize() throws IOException, DatasetManagementException,
    DatasetNotFoundException, ClassNotFoundException {
    dataset = datasetInstantiator.getDataset(datasetId);
    if (dataset instanceof TransactionAware) {
      ((TransactionAware) dataset).startTx(transaction);
    }
  }

  public <T extends Dataset> T getDataset() {
    return (T) dataset;
  }

  @Override
  public void close() throws IOException {
    datasetInstantiator.close();
    context.close();
  }
}
