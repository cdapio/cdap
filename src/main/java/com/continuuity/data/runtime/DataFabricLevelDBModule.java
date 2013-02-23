/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import java.io.File;
import java.util.Random;

import org.jruby.util.io.DirectoryAsFileException;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
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

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLevelDBModule extends AbstractModule {

  private final String basePath;

  public DataFabricLevelDBModule() {
    this(CConfiguration.create());
  }

  public DataFabricLevelDBModule(CConfiguration configuration) {
    String path = configuration.get("data.local.leveldb");
    if (path == null || path.isEmpty()) {
      path =
        System.getProperty("java.io.tmpdir") +
        System.getProperty("file.separator") +
        "ldb-test-" + Long.toString(System.currentTimeMillis());
      if (!new File(path).mkdirs()) {
        throw new RuntimeException("Unable to create directory for ldb");
      }
    }

    this.basePath = path;
  }

  public DataFabricLevelDBModule(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public void configure() {

    // Bind our implementations

    // There is only one timestamp oracle for the whole system
    bind(TimestampOracle.class).
        to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);

    bind(TransactionOracle.class).to(MemoryOracle.class);

    // This is the primary mapping of the data fabric to underlying storage
    bind(OVCTableHandle.class).to(LevelDBOVCTableHandle.class);
    
    bind(OperationExecutor.class).
        to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
    
    // Bind named fields
    
    bind(String.class)
        .annotatedWith(Names.named("LevelDBOVCTableHandleBasePath"))
        .toInstance(basePath);
  }
}
