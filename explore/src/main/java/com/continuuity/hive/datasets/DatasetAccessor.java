package com.continuuity.hive.datasets;

import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.data2.dataset2.manager.DatasetManagementException;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.hive.context.Constants;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.TxnSerDe;
import com.continuuity.internal.data.dataset.Dataset;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Helps in instantiating a dataset.
 */
public class DatasetAccessor {

  // TODO: this will go away when dataset manager does not return datasets having classloader conflict.
  private static final Map<String, ClassLoader> datasetClassloaders = Maps.newHashMap();

  public static RowScannable getRowScannable(Configuration conf) throws IOException {
    RowScannable rowScannable = instantiate(conf);

    if (rowScannable instanceof TransactionAware) {
      // TODO: do we have to commit transaction?
      Transaction tx = TxnSerDe.deserialize(conf);
      ((TransactionAware) rowScannable).startTx(tx);
    }

    return rowScannable;
  }

  public static Type getRowScannableType(Configuration conf) throws IOException {
    return instantiate(conf).getRowType();
  }

  private static RowScannable instantiate(Configuration conf) throws IOException {
    String datasetName = conf.get(Constants.DATASET_NAME);
    if (datasetName == null) {
      throw new IOException(String.format("Dataset name property %s not defined.", Constants.DATASET_NAME));
    }

    DatasetManager manager = ContextManager.getDatasetManager(conf);

    try {
      ClassLoader classLoader = datasetClassloaders.get(datasetName);
      if (classLoader == null) {
        classLoader = conf.getClassLoader();
      }

      Dataset dataset = manager.getDataset(datasetName, classLoader);
      if (dataset != null && !datasetClassloaders.containsKey(datasetName)) {
        datasetClassloaders.put(datasetName, dataset.getClass().getClassLoader());
      }

      if (!(dataset instanceof RowScannable)) {
        throw new IOException(
          String.format("Dataset %s does not implement RowScannable, and hence cannot be queried in Hive.",
                        datasetName));
      }

      return (RowScannable) dataset;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    }
  }
}
