/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBAndMemoryOVCTableHandle;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.inmemory.StatePersistor;
import com.continuuity.data2.transaction.inmemory.ZooKeeperPersistor;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBAndInMemoryQueueAdmin;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBAndInMemoryQueueClientFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;

import java.io.File;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLocalModule extends AbstractModule {

  private final CConfiguration conf;

  public DataFabricLocalModule() {
    this(CConfiguration.create());
  }

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
        if (conf.getBoolean(StatePersistor.CFG_DO_PERSIST, true)) {
          bind(StatePersistor.class).to(ZooKeeperPersistor.class).in(Singleton.class);
        }
        bind(LevelDBOVCTableHandle.class).toInstance(LevelDBOVCTableHandle.getInstance());
        bind(MemoryOVCTableHandle.class).toInstance(MemoryOVCTableHandle.getInstance());
        bind(OVCTableHandle.class).toInstance(new LevelDBAndMemoryOVCTableHandle());
        bind(QueueClientFactory.class).to(LevelDBAndInMemoryQueueClientFactory.class).in(Singleton.class);
        bind(QueueAdmin.class).to(LevelDBAndInMemoryQueueAdmin.class).in(Singleton.class);
      }
    }));
  }

} // end of DataFabricLocalModule
