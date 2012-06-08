package com.continuuity.data.table;

import com.continuuity.data.runtime.DataFabricInMemoryModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestMemoryOVCTable extends TestOVCTable {

  private static final Injector injector =
      Guice.createInjector(new DataFabricInMemoryModule());

  @Override
  protected OVCTableHandle getTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

}
