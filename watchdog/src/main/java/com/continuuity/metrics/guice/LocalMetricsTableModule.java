/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Guice module to provide bindings for metrics table access in local mode. It's intend to be
 * installed by other private module in this package.
 */
public final class LocalMetricsTableModule extends AbstractMetricsTableModule {

  private static final Logger LOG = LoggerFactory.getLogger(LocalMetricsTableModule.class);

  @Override
  protected void bindTableHandle() {
    bind(OVCTableHandle.class).toInstance(LevelDBOVCTableHandle.getInstance());
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

    LOG.info("LevelDB path: {}", p);

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
