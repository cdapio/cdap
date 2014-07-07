/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data.stream.InMemoryStreamCoordinator;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueClientFactory;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryStreamAdmin;
import com.continuuity.data2.transaction.runtime.TransactionModules;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.data2.transaction.stream.inmemory.InMemoryStreamConsumerFactory;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.metadata.SerializingMetaDataTable;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

/**
 * The Guice module of data fabric bindings for in memory execution.
 */
public class DataFabricInMemoryModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MetaDataTable.class).to(SerializingMetaDataTable.class).in(Singleton.class);

    // Bind TxDs2 stuff

    bind(DataSetAccessor.class).to(InMemoryDataSetAccessor.class).in(Singleton.class);
    bind(QueueClientFactory.class).to(InMemoryQueueClientFactory.class).in(Singleton.class);
    bind(QueueAdmin.class).to(InMemoryQueueAdmin.class).in(Singleton.class);
    bind(StreamAdmin.class).to(InMemoryStreamAdmin.class).in(Singleton.class);

    bind(StreamCoordinator.class).to(InMemoryStreamCoordinator.class).in(Singleton.class);
    bind(StreamConsumerFactory.class).to(InMemoryStreamConsumerFactory.class).in(Singleton.class);
    bind(StreamFileWriterFactory.class).to(InMemoryStreamFileWriterFactory.class).in(Singleton.class);

    // bind transactions
    install(new TransactionModules().getInMemoryModules());
  }
}
