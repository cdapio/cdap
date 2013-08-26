/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * HBase queue tests.
 */
public class InMemoryQueueTest extends QueueTest {

  private static InMemoryQueueService queueService;

  @BeforeClass
  public static void init() throws Exception {

    final Module dataFabricModule = new DataFabricModules().getInMemoryModules();
    final Injector injector = Guice.createInjector(dataFabricModule);
    // Get the in-memory opex
    opex = injector.getInstance(OperationExecutor.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);

    queueService = injector.getInstance(InMemoryQueueService.class);
  }

  @AfterClass
  public static void finish() {
    queueService.dumpInfo(System.out);
  }
}
