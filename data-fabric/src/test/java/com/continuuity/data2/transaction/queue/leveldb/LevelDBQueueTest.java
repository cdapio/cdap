/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueTest;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * HBase queue tests.
 */
public class LevelDBQueueTest extends QueueTest {

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    Injector injector = Guice.createInjector(new DataFabricLevelDBModule(conf));
    // Get the in-memory opex
    opex = injector.getInstance(OperationExecutor.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
  }
}
