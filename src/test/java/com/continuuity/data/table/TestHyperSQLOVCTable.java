package com.continuuity.data.table;

import com.continuuity.data.engine.hypersql.HyperSQLOVCTable;
import com.continuuity.data.runtime.DataFabricLocalModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import static org.junit.Assert.assertTrue;

public class TestHyperSQLOVCTable extends TestOVCTable {

  private static final Injector injector = Guice.createInjector (
      new DataFabricLocalModule("jdbc:hsqldb:mem:membenchdb", null));
  // Guice.createInjector(new DataFabricLocalModule());

  @Override
  protected OVCTableHandle injectTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

  @Override
  public void testInjection() {
    assertTrue(table instanceof HyperSQLOVCTable);
  }

}
