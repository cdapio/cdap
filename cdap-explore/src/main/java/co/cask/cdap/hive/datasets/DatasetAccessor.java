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

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.explore.service.ExploreService;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.context.NullJobConfException;
import co.cask.cdap.hive.context.TxnCodec;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helps in instantiating a dataset.
 */
public class DatasetAccessor {
  /**
   * Indicates whether {@link DatasetAccessor} is being run from {@link ExploreService}, or from a Hive query. 
   */
  public enum RunContext {
    EXPLORE_SERVICE,
    QUERY
  }
  
  // TODO: this will go away when dataset manager does not return datasets having classloader conflict - CDAP-10
  private static final Map<String, ClassLoader> DATASET_CLASSLOADERS = Maps.newConcurrentMap();

  // By default the run context is assumed to be Query.
  private static RunContext runContext = RunContext.QUERY;
  
  public static void setRunContext(RunContext runContext) {
    DatasetAccessor.runContext = runContext;
  }

  /**
   * Returns a RecordScannable dataset. The returned object will have to be closed by the caller.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configuration.
   * @return RecordScannable which name is contained in the {@code conf}.
   * @throws IOException in case the conf does not contain a valid RecordScannable.
   */
  public static RecordScannable getRecordScannable(Configuration conf) throws IOException {
    Dataset dataset = instantiate(conf);

    if (!(dataset instanceof RecordScannable)) {
      throw new IOException(
        String.format("Dataset %s does not implement RecordScannable, and hence cannot be queried in Hive.",
                      conf.get(Constants.Explore.DATASET_NAME)));
    }

    RecordScannable recordScannable = (RecordScannable) dataset;

    if (recordScannable instanceof TransactionAware) {
      startTransaction(conf, (TransactionAware) recordScannable);
    }

    return recordScannable;
  }

  /**
   * Returns a RecordWritable dataset. The returned object will have to be closed by the caller.
   *
   * @param conf Configuration that contains RecordWritable name to load, CDAP and HBase configurations.
   * @return RecordWritable which name is contained in the {@code conf}.
   * @throws IOException in case the conf does not contain a valid RecordWritable.
   */
  public static RecordWritable getRecordWritable(Configuration conf) throws IOException {
    RecordWritable recordWritable = instantiateWritable(conf, null);

    if (recordWritable instanceof TransactionAware) {
      startTransaction(conf, (TransactionAware) recordWritable);
    }
    return recordWritable;
  }

  /**
   * Check that the conf contains information about a valid RecordWritable object.
   * @param conf configuration containing RecordWritable name, CDAP and HBase configurations.
   * @throws IOException in case the conf does not contain a valid RecordWritable.
   */
  public static void checkRecordWritable(Configuration conf) throws IOException {
    RecordWritable recordWritable = instantiateWritable(conf, null);
    if (recordWritable != null) {
      recordWritable.close();
    }
  }

  /**
   * Returns record type of the RecordScannable.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configurations.
   * @return Record type of RecordScannable dataset.
   * @throws IOException in case the conf does not contain a valid RecordScannable.
   */
  public static Type getRecordType(Configuration conf) throws IOException {
    Dataset dataset = instantiate(conf);
    try {
      if (dataset instanceof RecordWritable) {
        return ((RecordWritable) dataset).getRecordType();
      } else if (dataset instanceof RecordScannable) {
        return ((RecordScannable) dataset).getRecordType();
      }
      throw new IOException(
        String.format("Dataset %s does not implement neither RecordScannable nor RecordWritable.",
                      conf.get(Constants.Explore.DATASET_NAME)));
    } finally {
      dataset.close();
    }
  }

  /**
   * Returns record type of the RecordWritable. Calling this method assumes that a class loader has already but
   * cached to load the writable. If not, a {@link co.cask.cdap.hive.context.NullJobConfException} will be trown.
   *
   * @param datasetName dataset name to load.
   * @return Record type of RecordWritable dataset.
   * @throws IOException in case the {@code datasetName} does not reference a RecordWritable.
   */
  public static Type getRecordWritableType(String datasetName) throws IOException {
    RecordWritable<?> recordWritable = instantiateWritable(null, datasetName);
    try {
      return recordWritable.getRecordType();
    } finally {
      recordWritable.close();
    }
  }

  private static void startTransaction(Configuration conf, TransactionAware txAware) throws IOException {
    Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
    txAware.startTx(tx);
  }

  private static RecordWritable instantiateWritable(Configuration conf, @Nullable String datasetName)
    throws IOException {
    Dataset dataset = instantiate(conf, datasetName);

    if (!(dataset instanceof RecordWritable)) {
      dataset.close();
      throw new IOException(
        String.format("Dataset %s does not implement RecordWritable, and hence cannot be written to in Hive.",
                      datasetName != null ? datasetName : conf.get(Constants.Explore.DATASET_NAME)));
    }
    return (RecordWritable) dataset;
  }

  private static Dataset instantiate(Configuration conf) throws IOException {
    Dataset dataset = instantiate(conf, null);

    if (!(dataset instanceof RecordScannable || dataset instanceof RecordWritable)) {
      throw new IOException(
        String.format("Dataset %s does not implement neither RecordScannable nor RecordWritable.",
                      conf.get(Constants.Explore.DATASET_NAME)));
    }
    return dataset;
  }

  private static Dataset instantiate(Configuration conf, @Nullable String dsName)
    throws IOException {
    ContextManager.Context context = ContextManager.getContext(conf);
    String datasetName = dsName != null ? dsName : conf.get(Constants.Explore.DATASET_NAME);

    if (datasetName == null) {
      throw new IOException("Dataset name property could not be found.");
    }

    try {
      DatasetFramework framework = context.getDatasetFramework();
      
      // Do not cache the Dataset classloader if running as part of Explore Service.
      if (runContext == RunContext.EXPLORE_SERVICE) {
        return framework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, conf.getClassLoader());
      }

      return cacheLoad(conf, datasetName, framework);
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    } finally {
      context.close();
    }
  }
  
  private static Dataset cacheLoad(Configuration conf, String datasetName, DatasetFramework framework) 
    throws IOException, DatasetManagementException {
    ClassLoader classLoader = DATASET_CLASSLOADERS.get(datasetName);
    Dataset dataset;
    if (classLoader == null) {
      if (conf == null) {
        throw new NullJobConfException();
      }
      classLoader = conf.getClassLoader();
      dataset = firstLoad(framework, datasetName, classLoader);
    } else {
      dataset = framework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, classLoader);
    }
    return dataset;
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
