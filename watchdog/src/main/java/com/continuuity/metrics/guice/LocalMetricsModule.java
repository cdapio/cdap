/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.collect.LocalMetricsCollectionService;
import com.continuuity.metrics.collect.MetricsCollectionService;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

import java.io.File;

/**
 * Guice module to provide bindings for local metrics system.
 */
final class LocalMetricsModule extends AbstractMetricsModule {

  @Override
  protected void bindTableHandle() {
    bind(OVCTableHandle.class).toInstance(LevelDBOVCTableHandle.getInstance());
    bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Named("LevelDBOVCTableHandleBasePath")
  public String providesBasePath(CConfiguration cConf) {
    String path = cConf.get(Constants.CFG_DATA_LEVELDB_DIR);
    if (path == null || path.isEmpty()) {
      path = System.getProperty("java.io.tmpdir") +
             System.getProperty("file.separator") +
             "ldb-test-" + Long.toString(System.currentTimeMillis());
    }

    File p = new File(path);
    if (!p.exists() && !p.mkdirs() && !p.exists()) {
      throw new RuntimeException("Unable to create directory for ldb");
    }

    return p.getAbsolutePath();
  }

  @Provides
  @Named("LevelDBOVCTableHandleBlockSize")
  public Integer providesBlockSize(CConfiguration cConf) {
    return cConf.getInt(Constants.CFG_DATA_LEVELDB_BLOCKSIZE, Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE);
  }

  @Provides
  @Named("LevelDBOVCTableHandleCacheSize")
  public Long providesCacheSize(CConfiguration cConf) {
    return cConf.getLong(Constants.CFG_DATA_LEVELDB_CACHESIZE, Constants.DEFAULT_DATA_LEVELDB_CACHESIZE);
  }
}
