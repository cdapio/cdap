package com.continuuity.data;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class LocalDataSetAccessorTest extends NamespacingDataSetAccessorTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static DataSetAccessor dsAccessor;

  @BeforeClass
  public static void beforeClass() throws Exception {
    NamespacingDataSetAccessorTest.beforeClass();
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      new LocationRuntimeModule().getSingleNodeModules(),
      new DataFabricLevelDBModule(conf));
    dsAccessor = injector.getInstance(DataSetAccessor.class);
  }

  @Override
  protected DataSetAccessor getDataSetAccessor() {
    return dsAccessor;
  }
}
