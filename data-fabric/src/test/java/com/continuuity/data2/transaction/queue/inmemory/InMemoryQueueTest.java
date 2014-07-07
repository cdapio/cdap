/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueTest;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * In-memory queue tests.
 */
public class InMemoryQueueTest extends QueueTest {

  private static Injector injector;

  @BeforeClass
  public static void init() throws Exception {

    injector = Guice.createInjector(new LocationRuntimeModule().getInMemoryModules(),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new DataFabricModules().getInMemoryModules(),
                                    new DataSetsModules().getInMemoryModule(),
                                    new TransactionMetricsModule());
    // transaction manager is a "service" and must be started
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
    queueAdmin = injector.getInstance(QueueAdmin.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }
}
