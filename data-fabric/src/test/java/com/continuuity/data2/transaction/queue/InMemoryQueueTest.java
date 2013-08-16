/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * HBase queue tests.
 */
public class InMemoryQueueTest extends QueueTest {

  @BeforeClass
  public static void init() throws Exception {

    final Module dataFabricModule = new DataFabricModules().getInMemoryModules();
    final Injector injector = Guice.createInjector(dataFabricModule);
    // Get the in-memory opex
    opex = injector.getInstance(OperationExecutor.class);
  }

  @Override
  protected Queue2Producer createProducer(String tableName, QueueName queueName) throws IOException {
    return new InMemoryQueue2Producer(queueName);
  }

  @Override
  protected Queue2Consumer createConsumer(String tableName, QueueName queueName, ConsumerConfig config)
    throws IOException {
    return new InMemoryQueue2Consumer(queueName, config);
  }
}
