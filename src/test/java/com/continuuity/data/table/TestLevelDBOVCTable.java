package com.continuuity.data.table;

import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestLevelDBOVCTable extends TestOVCTable {

  private static final Injector injector = Guice.createInjector (
      new DataFabricLevelDBModule());

  @Override
  protected OVCTableHandle injectTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

}
