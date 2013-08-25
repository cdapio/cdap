/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

/**
 * HBase queue tests.
 */
public class LevelDBQueueTest extends QueueTest {

  static LevelDBOcTableService service;

  @BeforeClass
  public static void configure() throws IOException {

    final Module dataFabricModule = new DataFabricModules().getInMemoryModules();
    final Injector injector = Guice.createInjector(dataFabricModule);
    // Get the in-memory opex
    opex = injector.getInstance(OperationExecutor.class);

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
    service.createTable(LevelDBQueueClientFactory.QUEUE_TABLE_NAME);
    queueClientFactory = new LevelDBQueueClientFactory(service);
  }
}
