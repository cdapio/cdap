package com.continuuity.data.metadata;

import com.continuuity.data.engine.memory.MemoryOVCTableHandle;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;

import static org.junit.Assert.assertTrue;

/**
 * Memory metadata store tests.
 */
public abstract class MemoryMetaDataStoreTest extends MetaDataStoreTest {

  protected static Injector injector;

  @BeforeClass
  public static void setupDataFabric() throws Exception {

    injector = Guice.createInjector (
        new DataFabricModules().getInMemoryModules());
  }

  @Override
  public void testInjection() {
    assertTrue(injector.getInstance(OVCTableHandle.class) instanceof MemoryOVCTableHandle);
  }
}
