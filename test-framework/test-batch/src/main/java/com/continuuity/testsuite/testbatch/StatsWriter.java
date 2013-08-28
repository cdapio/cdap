package com.continuuity.testsuite.testbatch;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.sql.Timestamp;


/**
 * Store the incoming Purchase Objects in datastore.
 */
public class StatsWriter extends AbstractFlowlet {

  @UseDataSet("key-value")
  private KeyValueTable kvTable;

  @UseDataSet("file-stats")
  private ObjectStore<FileStat> fileStats;

  /**
   *
   * @param fileStat
   * @throws OperationException
   * Write file stats to Object dataStore and key value Counter
   */
  public void process(FileStat fileStat) throws OperationException {

    // Save to ObjectStore, timestamp as the key
    fileStats.write(Bytes.toBytes(System.currentTimeMillis()), fileStat);
  }
}
