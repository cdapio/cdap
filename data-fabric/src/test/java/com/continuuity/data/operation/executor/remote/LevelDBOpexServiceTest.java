package com.continuuity.data.operation.executor.remote;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class LevelDBOpexServiceTest extends OperationExecutorServiceTest {

  static Injector injector;

  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    injector = Guice.createInjector (
        new DataFabricLevelDBModule(conf));
    OperationExecutorServiceTest.startService(
        conf, injector.getInstance(OperationExecutor.class));
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof LevelDBOVCTableHandle);
  }
}
