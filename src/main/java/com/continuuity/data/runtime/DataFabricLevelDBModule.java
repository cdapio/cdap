/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBAndMemoryOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

import java.io.File;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLevelDBModule extends AbstractModule {

  private final String basePath;
  private final Integer blockSize;
  private final Long cacheSize;

  public DataFabricLevelDBModule() {
    this(CConfiguration.create());
  }

  public DataFabricLevelDBModule(CConfiguration configuration) {
    String path = configuration.get(Constants.CFG_DATA_LEVELDB_DIR,
                                    Constants.DEFAULT_DATA_LEVELDB_DIR);
    if (path == null || path.isEmpty()) {
      path =
        System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") +
        "ldb-test-" + Long.toString(System.currentTimeMillis());
    }
    if (!new File(path).mkdirs()) {
      throw new RuntimeException("Unable to create directory for ldb");
    }
    this.basePath = path;
    this.blockSize = configuration.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE,
                                          Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE);
    this.cacheSize = configuration.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE,
                                           Constants.DEFAULT_DATA_LEVELDB_CACHESIZE);
  }

  public DataFabricLevelDBModule(String basePath, Integer blockSize,
      Long cacheSize) {
    this.basePath = basePath;
    this.blockSize = blockSize;
    this.cacheSize = cacheSize;
  }

  @Override
  public void configure() {

    // Bind our implementations

    // There is only one timestamp oracle for the whole system
    bind(TimestampOracle.class).
        to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);

    bind(TransactionOracle.class).to(MemoryOracle.class);

    // This is the primary mapping of the data fabric to underlying storage
    bind(OVCTableHandle.class).to(LevelDBAndMemoryOVCTableHandle.class);
    
    bind(OperationExecutor.class).
        to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
    
    // Bind named fields
    
    bind(String.class)
        .annotatedWith(Names.named("LevelDBOVCTableHandleBasePath"))
        .toInstance(basePath);
    
    bind(Integer.class)
        .annotatedWith(Names.named("LevelDBOVCTableHandleBlockSize"))
        .toInstance(blockSize);
    
    bind(Long.class)
        .annotatedWith(Names.named("LevelDBOVCTableHandleCacheSize"))
        .toInstance(cacheSize);
    
  }
}
