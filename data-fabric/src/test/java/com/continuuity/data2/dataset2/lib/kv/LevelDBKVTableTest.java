package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class LevelDBKVTableTest extends NoTxKeyValueTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  static LevelDBOcTableService service;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    conf.set(Constants.CFG_DATA_LEVELDB_DIR, tmpFolder.newFolder().getAbsolutePath());
    service = new LevelDBOcTableService();
    service.setConfiguration(conf);
  }

  @Override
  protected DatasetDefinition<? extends NoTxKeyValueTable, ? extends DatasetAdmin> getDefinition() throws IOException {
    final String dataDir = tmpFolder.newFolder().getAbsolutePath();
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        CConfiguration conf = CConfiguration.create();
        conf.set(Constants.CFG_DATA_LEVELDB_DIR, dataDir);
        bind(CConfiguration.class).toInstance(conf);
        bind(LevelDBOcTableService.class).toInstance(service);
      }
    });

    LevelDBKVTableDefinition def = new LevelDBKVTableDefinition("foo");
    injector.injectMembers(def);
    return def;
  }
}
