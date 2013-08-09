package com.continuuity.data.metadata;

import com.continuuity.data.engine.hypersql.HyperSQLOVCTableHandle;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.util.OperationUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

/**
 * Hypersql backed metadata store tests.
 */
public abstract class HyperSQLMetaDataStoreTest extends MetaDataStoreTest {

  private static Injector injector;

  @BeforeClass
  public static void setupOpex() throws Exception {
    injector = Guice.createInjector (
        new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
    opex = injector.getInstance(OperationExecutor.class);
    opex.execute(OperationUtil.DEFAULT,
        new ClearFabric(ClearFabric.ToClear.ALL));
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof HyperSQLOVCTableHandle);
  }
}
