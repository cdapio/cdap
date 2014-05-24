/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.datafabric.dataset.DataFabricDatasetManager;
import com.continuuity.data2.datafabric.dataset.hive.HiveDatasetTableManager;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.inmemory.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetManager;
import com.continuuity.data2.dataset2.module.lib.TableModule;
import com.continuuity.data2.dataset2.module.lib.leveldb.LevelDBTableModule;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.persist.LocalFileTransactionStateStorage;
import com.continuuity.data2.transaction.persist.NoOpTransactionStateStorage;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueAdmin;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBAndInMemoryQueueClientFactory;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.NavigableMap;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLocalModule extends AbstractModule {

  private final CConfiguration conf;

  public DataFabricLocalModule(CConfiguration conf) {
    this.conf = conf;

    String path = conf.get(Constants.CFG_DATA_LEVELDB_DIR);
    if (path == null || path.isEmpty()) {
      path =
        System.getProperty("java.io.tmpdir") +
          System.getProperty("file.separator") +
          "ldb-test-" + Long.toString(System.currentTimeMillis());
      conf.set(Constants.CFG_DATA_LEVELDB_DIR, path);
    }

    File p = new File(path);
    if (!p.exists() && !p.mkdirs()) {
      throw new RuntimeException("Unable to create directory for ldb");
    }
    p.deleteOnExit();
  }

  @Override
  public void configure() {

    install(Modules.override(new DataFabricLevelDBModule(this.conf)).with(new AbstractModule() {
      @Override
      protected void configure() {
        if (conf.getBoolean(Constants.Transaction.Manager.CFG_DO_PERSIST, true)) {
          bind(TransactionStateStorage.class).to(LocalFileTransactionStateStorage.class).in(Singleton.class);
        } else {
          bind(TransactionStateStorage.class).to(NoOpTransactionStateStorage.class).in(Singleton.class);
        }
        bind(QueueClientFactory.class).to(LevelDBAndInMemoryQueueClientFactory.class).in(Singleton.class);
        bind(QueueAdmin.class).to(InMemoryQueueAdmin.class).in(Singleton.class);

        // Bind hive table manager
        bind(HiveDatasetTableManager.class).in(Singleton.class);

        install(new DataSetsModules().getLocalModule());
      }
    }));
  }

} // end of DataFabricLocalModule
