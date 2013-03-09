package com.continuuity.data.operation.executor.remote;

import org.junit.BeforeClass;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class LevelDBOpexServiceTest extends OperationExecutorServiceTest {

  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    Injector injector = Guice.createInjector (
        new DataFabricLevelDBModule(conf));
    OperationExecutorServiceTest.startService(
        conf, injector.getInstance(OperationExecutor.class));
  }

}
