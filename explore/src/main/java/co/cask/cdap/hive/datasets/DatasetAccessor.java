/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.context.TxnCodec;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionAware;
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
  private static final Map<String, ClassLoader> DATASET_CLASSLOADERS = Maps.newConcurrentMap();

  /**
   * Returns a RecordScannable. The returned object will have to be closed by the caller.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configuration.
   * @return RecordScannable.
   * @throws IOException
   */
  public static RecordScannable getRecordScannable(Configuration conf) throws IOException {
    RecordScannable recordScannable = instantiateScannable(conf);

    if (recordScannable instanceof TransactionAware) {
      Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
        ((TransactionAware) recordScannable).startTx(tx);
    }

    return recordScannable;
  }

  public static RecordWritable getRecordWritable(Configuration conf) throws IOException {
    // TODO make it consistent with getRecordScannable
    RecordWritable recordWritable = instantiateWritable(conf, null);

    // TODO do extra work on transactions
    if (recordWritable instanceof TransactionAware) {
      Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
      ((TransactionAware) recordWritable).startTx(tx);
    }
    return recordWritable;
  }

  /**
   * Returns record type of the RecordScannable.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configuration.
   * @return Record type of RecordScannable.
   * @throws IOException
   */
  public static Type getRecordScannableType(Configuration conf) throws IOException {
    RecordScannable<?> recordScannable = instantiateScannable(conf);
    try {
      return recordScannable.getRecordType();
    } finally {
      recordScannable.close();
    }
  }

  public static Type getRecordWritableType(Configuration conf, String datasetName) throws IOException {
    // conf can be null

    RecordWritable<?> recordWritable = instantiateWritable(conf, datasetName);
    try {
      return recordWritable.getRecordType();
    } finally {
      recordWritable.close();
    }
  }

  private static RecordWritable instantiateWritable(Configuration conf, String datasetName) throws IOException {
    // conf can be null

    Dataset dataset = instantiate(conf, datasetName);

    if (!(dataset instanceof RecordWritable)) {
      throw new IOException(
        String.format("Dataset %s does not implement RecordWritable, and hence cannot be written to in Hive.",
                      conf.get(Constants.Explore.DATASET_NAME)));
    }
    return (RecordWritable) dataset;
  }

  private static RecordScannable instantiateScannable(Configuration conf) throws IOException {
    Dataset dataset = instantiate(conf, null);

    if (!(dataset instanceof RecordScannable)) {
      throw new IOException(
        String.format("Dataset %s does not implement RecordScannable, and hence cannot be queried in Hive.",
                      conf.get(Constants.Explore.DATASET_NAME)));
    }
    return (RecordScannable) dataset;
  }

  private static Dataset instantiate(Configuration conf, String dsName) throws IOException {
    String datasetName = dsName;
    if (datasetName == null) {
      datasetName = conf.get(Constants.Explore.DATASET_NAME);
      if (datasetName == null) {
        throw new IOException(String.format("Dataset name property %s not defined.", Constants.Explore.DATASET_NAME));
      }
    }

    ContextManager.Context context = ContextManager.getContext(conf);

    try {
      DatasetFramework framework = context.getDatasetFramework();

      ClassLoader classLoader = DATASET_CLASSLOADERS.get(datasetName);
      Dataset dataset;
      if (classLoader == null) {
        classLoader = conf.getClassLoader();
        dataset = firstLoad(framework, datasetName, classLoader);
      } else {
        dataset = framework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, classLoader);
      }
      return dataset;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    } finally {
      context.close();
    }
  }

  private static synchronized Dataset firstLoad(DatasetFramework framework, String datasetName, ClassLoader classLoader)
    throws DatasetManagementException, IOException {
    ClassLoader datasetClassLoader = DATASET_CLASSLOADERS.get(datasetName);
    if (datasetClassLoader != null) {
      // Some other call in parallel may have already loaded it, so use the same classlaoder
      return framework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, datasetClassLoader);
    }

    // No classloader for dataset exists, load the dataset and save the classloader.
    Dataset dataset = framework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, classLoader);
    if (dataset != null) {
      DATASET_CLASSLOADERS.put(datasetName, dataset.getClass().getClassLoader());
    }
    return dataset;
  }
}
