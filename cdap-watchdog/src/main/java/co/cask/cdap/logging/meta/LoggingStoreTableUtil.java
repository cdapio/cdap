/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.meta;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetManager;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;

/**
 * Utility class for helper functions to setup meta data table used by the logging system.
 */
public final class LoggingStoreTableUtil {

  // The row key prefix for rows that stores CDAP log files metadata.
  public static final byte[] OLD_FILE_META_ROW_KEY_PREFIX = Bytes.toBytes(200);
  public static final byte[] NEW_FILE_META_ROW_KEY_PREFIX = Bytes.toBytes(300);
  public static final byte[] META_TABLE_COLUMN_KEY = Bytes.toBytes("file");
  private static final DatasetId META_TABLE_DATASET_ID = NamespaceId.SYSTEM.dataset(Constants.Logging.META_TABLE);

  /**
   * Setups the checkpoint data table dataset. This method should only be called from UpgradeTool.
   */
  public static void setupDatasets(DatasetFramework dsFramework) throws DatasetManagementException, IOException {
    dsFramework.addInstance(Table.class.getName(), META_TABLE_DATASET_ID, DatasetProperties.EMPTY);
  }

  /**
   * Returns the {@link Table} for storing metadata.
   */
  public static Table getMetadataTable(DatasetFramework datasetFramework,
                                       DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, META_TABLE_DATASET_ID,
                                           Table.class.getName(), DatasetProperties.EMPTY);
  }

  /**
   * Returns the {@link Table} for storing metadata.
   */
  public static Table getMetadataTable(DatasetContext context,
                                       DatasetManager manager) throws DatasetManagementException {
    DatasetId datasetId = META_TABLE_DATASET_ID;
    try {
      return context.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    } catch (DatasetInstantiationException e) {
      try {
        manager.createDataset(datasetId.getDataset(), Table.class.getName(), DatasetProperties.EMPTY);
      } catch (InstanceConflictException ie) {
        // This is ok if the dataset already exists
      }
      return context.getDataset(datasetId.getNamespace(), datasetId.getDataset());
    }
  }

  private LoggingStoreTableUtil() {
    // No-op
  }
}
