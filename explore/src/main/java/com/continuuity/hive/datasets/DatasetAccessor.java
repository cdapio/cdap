package com.continuuity.hive.datasets;

import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.hive.server.RuntimeHiveServer;
import com.continuuity.internal.data.dataset.Dataset;

import java.io.IOException;

/**
 * Helps in instantiating a dataset.
 */
public class DatasetAccessor {
  public static Dataset getDataSetInstance(String datasetName, Transaction tx) throws IOException {
    DatasetManager manager = RuntimeHiveServer.getDatasetManager();
    try {
      Dataset dataset = manager.getDataset(datasetName, null);
      if (dataset instanceof TransactionAware) {
        // TODO: do we have to commit transaction?
        ((TransactionAware) dataset).startTx(tx);
      }
      return dataset;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    }
  }
}
