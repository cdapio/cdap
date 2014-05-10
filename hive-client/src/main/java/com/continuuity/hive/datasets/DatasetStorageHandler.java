package com.continuuity.hive.datasets;

import com.continuuity.api.data.batch.RowScannable;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * HiveStorageHandler to access Datasets.
 */
public class DatasetStorageHandler extends DefaultStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetStorageHandler.class);

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DatasetInputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return DatasetSerDe.class;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    String datasetName = tableDesc.getProperties().getProperty(DatasetInputFormat.DATASET_NAME);
    jobProperties.put(DatasetInputFormat.DATASET_NAME, datasetName);

    try {
      RowScannable rowScannable = DatasetInputFormat.getDataset(datasetName);
      jobProperties.put(DatasetSerDe.DATASET_ROW_TYPE, rowScannable.getRowType().toString());
    } catch (Exception e) {
      LOG.error("Got exception while instantiating dataset {}", datasetName, e);
    }
  }
}
