package com.continuuity.hive.datasets;

import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DatasetManagementException;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.hive.context.ConfigurationUtil;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.TxnCodec;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Helps in instantiating a dataset.
 */
public class DatasetAccessor {

  // TODO: this will go away when dataset manager does not return datasets having classloader conflict - REACTOR-276
  private static final Map<String, ClassLoader> DATASET_CLASSLOADERS = Maps.newHashMap();

  public static RowScannable getRowScannable(Configuration conf) throws IOException {
    RowScannable rowScannable = instantiate(conf);

    if (rowScannable instanceof TransactionAware) {
      Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
        ((TransactionAware) rowScannable).startTx(tx);
    }

    return rowScannable;
  }

  public static Type getRowScannableType(Configuration conf) throws IOException {
    return instantiate(conf).getRowType();
  }

  private static RowScannable instantiate(Configuration conf) throws IOException {
    String datasetName = conf.get(Constants.Explore.DATASET_NAME);
    if (datasetName == null) {
      throw new IOException(String.format("Dataset name property %s not defined.", Constants.Explore.DATASET_NAME));
    }

    DatasetFramework framework = ContextManager.getDatasetManager(conf);

    try {
      ClassLoader classLoader = DATASET_CLASSLOADERS.get(datasetName);
      if (classLoader == null) {
        classLoader = conf.getClassLoader();
      }

      Dataset dataset = framework.getDataset(datasetName, classLoader);
      if (dataset != null && !DATASET_CLASSLOADERS.containsKey(datasetName)) {
        DATASET_CLASSLOADERS.put(datasetName, dataset.getClass().getClassLoader());
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
