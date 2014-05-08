package com.continuuity.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * LevelDB backed metadata store tests.
 */
public abstract class LevelDBMetaDataStoreTest extends MetaDataTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
      new LocationRuntimeModule().getSingleNodeModules(),
      new DataFabricLevelDBModule(conf));
  }
}
