package com.continuuity.data2.datafabric.dataset.hive;

import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.data2.datafabric.dataset.DatasetsUtil;
import com.continuuity.hive.client.HiveClient;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Manager to create/delete/update hive tables with the
 */
public class HiveDatasetTableManager {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDatasetTableManager.class);

  private final Provider<HiveClient> hiveClient;

  @Inject
  public HiveDatasetTableManager(Provider<HiveClient> hiveClient) {
    this.hiveClient = hiveClient;
  }

  public <ROW> void createTable(String name, RowScannable<ROW> rowScannable) {
    String hiveStatement = DatasetsUtil.createHiveTable(name, rowScannable);
    if (hiveStatement != null) {
      try {
        hiveClient.get().sendCommand(hiveStatement);
      } catch (IOException e) {
        LOG.error("Failed to create hive table matching {} dataset", name, e);
      }
    }
  }
}
