package com.continuuity.data2.dataset.lib.table.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTable;
import com.continuuity.data2.dataset.lib.table.OrderedColumnarTableTest;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

/**
 * test for LevelDB tables.
 */
public class LevelDBOcTableClientTest extends OrderedColumnarTableTest {

  static LevelDBOcTableService service;

  @BeforeClass
  public static void configure() throws IOException {
    CConfiguration config = CConfiguration.create();
    String basePath =  System.getProperty("java.io.tmpdir") +
      System.getProperty("file.separator") + "ldb-test-" + Long.toString(System.currentTimeMillis());
    File p = new File(basePath);
    if (!p.exists() && !p.mkdirs()) {
      throw new RuntimeException("Unable to create directory for LevelDB");
    }
    p.deleteOnExit();
    config.set(Constants.CFG_DATA_LEVELDB_DIR, basePath);
    service = new LevelDBOcTableService(config);
  }

  @Override
  protected OrderedColumnarTable getTable(String name) throws IOException {
    return new LevelDBOcTableClient(name, service);
  }

  @Override
  protected DataSetManager getTableManager() throws IOException {
    return new LevelDBOcTableManager(service);
  }
}
