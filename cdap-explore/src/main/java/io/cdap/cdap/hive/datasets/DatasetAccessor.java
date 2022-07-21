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

package io.cdap.cdap.hive.datasets;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.DatasetNotFoundException;
import io.cdap.cdap.common.conf.ConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.hive.context.ContextManager;
import io.cdap.cdap.hive.context.TxnCodec;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;

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
  private final DatasetId datasetId;
  private final ContextManager.Context context;
  private final Transaction transaction;
  private final SystemDatasetInstantiator datasetInstantiator;
  private Dataset dataset;

  public DatasetAccessor(Configuration conf) throws IOException {
    String datasetName = conf.get(Constants.Explore.DATASET_NAME);
    String namespace = conf.get(Constants.Explore.DATASET_NAMESPACE);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetName), "dataset name not present in config");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "namespace not present in config");

    this.datasetId = new DatasetId(namespace, datasetName);
    this.context = ContextManager.getContext(conf);
    Preconditions.checkNotNull(context);
    this.datasetInstantiator = context.createDatasetInstantiator(conf.getClassLoader());
    this.transaction = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
  }

  public void initialize() throws IOException, DatasetManagementException,
    DatasetNotFoundException, ClassNotFoundException {
    dataset = datasetInstantiator.getDataset(datasetId);
    if (dataset instanceof TransactionAware) {
      ((TransactionAware) dataset).startTx(transaction);
    }
  }

  public DatasetId getDatasetId() {
    return datasetId;
  }

  public <T extends Dataset> T getDataset() {
    return (T) dataset;
  }

  public DatasetSpecification getDatasetSpec() throws DatasetManagementException, UnauthorizedException {
    return context.getDatasetSpec(datasetId);
  }

  @Override
  public void close() throws IOException {
    datasetInstantiator.close();
    context.close();
  }
}
