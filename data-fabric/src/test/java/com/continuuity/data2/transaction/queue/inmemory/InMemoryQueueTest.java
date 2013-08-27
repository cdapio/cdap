/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueTest;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * HBase queue tests.
 */
public class InMemoryQueueTest extends QueueTest {

  private static Injector injector;

  @BeforeClass
  public static void init() throws Exception {

    injector = Guice.createInjector(new DataFabricModules().getInMemoryModules());
    // Get the in-memory opex
    opex = injector.getInstance(OperationExecutor.class);
    queueClientFactory = injector.getInstance(QueueClientFactory.class);
  }
}
