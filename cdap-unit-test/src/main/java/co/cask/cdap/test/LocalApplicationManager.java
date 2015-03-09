/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.remote.RemoteApplicationManager;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.util.HashMap;

/**
 *
 */
public class LocalApplicationManager extends RemoteApplicationManager {


  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txSystemClient;

  public LocalApplicationManager(Id.Application application, ClientConfig clientConfig,
                                 DatasetFramework datasetFramework, TransactionSystemClient txSystemClient) {
    super(application, clientConfig);
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
  }

  @Override
  public <T> DataSetManager<T> getDataSet(String dataSetName) throws Exception {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(application.getNamespace(), dataSetName);
    @SuppressWarnings("unchecked")
    final T dataSet = (T) datasetFramework.getDataset(datasetInstanceId, new HashMap<String, String>(), null);
    try {
      final TransactionContext txContext;
      // not every dataset is TransactionAware. FileSets for example, are not transactional.
      if (dataSet instanceof TransactionAware) {
        TransactionAware txAwareDataset = (TransactionAware) dataSet;
        txContext = new TransactionContext(txSystemClient, Lists.newArrayList(txAwareDataset));
        txContext.start();
      } else {
        txContext = null;
      }
      return new DataSetManager<T>() {
        @Override
        public T get() {
          return dataSet;
        }

        @Override
        public void flush() {
          try {
            if (txContext != null) {
              txContext.finish();
              txContext.start();
            }
          } catch (TransactionFailureException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
