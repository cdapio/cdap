package com.continuuity.hive.datasets;

import com.continuuity.api.data.batch.RecordScannable;
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

  /**
   * Returns a RecordScannable. The returned object will have to be closed by the caller.
   *
   * @param conf Configuration that contains RecordScannable name to load, Reactor and HBase configuration.
   * @return RecordScannable.
   * @throws IOException
   */
  public static RecordScannable getRecordScannable(Configuration conf) throws IOException {
    RecordScannable recordScannable = instantiate(conf);

    if (recordScannable instanceof TransactionAware) {
      Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
        ((TransactionAware) recordScannable).startTx(tx);
    }

    return recordScannable;
  }

  /**
   * Returns record type of the RecordScannable.
   *
   * @param conf Configuration that contains RecordScannable name to load, Reactor and HBase configuration.
   * @return Record type of RecordScannable.
   * @throws IOException
   */
  public static Type getRecordScannableType(Configuration conf) throws IOException {
    RecordScannable<?> recordScannable = instantiate(conf);
    Type type = recordScannable.getRecordType();
    recordScannable.close();
    return type;
  }

  private static RecordScannable instantiate(Configuration conf) throws IOException {
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

      if (!(dataset instanceof RecordScannable)) {
        throw new IOException(
          String.format("Dataset %s does not implement RecordScannable, and hence cannot be queried in Hive.",
                        datasetName));
      }

      return (RecordScannable) dataset;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    }
  }
}
