package com.continuuity.data.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.data.util.OperationUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

public abstract class LevelDBMetaDataStoreTest extends MetaDataStoreTest {

  @BeforeClass
  public static void setupOpex() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.unset(Constants.CFG_DATA_LEVELDB_DIR);
    Injector injector = Guice.createInjector (
        new DataFabricLevelDBModule(conf));
    opex = injector.getInstance(OperationExecutor.class);
    opex.execute(OperationUtil.DEFAULT,
        new ClearFabric(ClearFabric.ToClear.ALL));
  }

}
