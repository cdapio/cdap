package com.continuuity.hive.datasets;

import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.hive.inmemory.LocalHiveServer;
import com.continuuity.internal.data.dataset.Dataset;

import java.io.IOException;

/**
 *
 */
public class DatasetAccessor {
  public static Dataset getDataSetInstance(String datasetName, Transaction tx) throws IOException {
    DatasetManager manager = LocalHiveServer.getDatasetManager();
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
