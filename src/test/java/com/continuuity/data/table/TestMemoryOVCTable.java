package com.continuuity.data.table;

import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestMemoryOVCTable extends TestOVCTable {

  private static final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());

  @Override
  protected OVCTableHandle injectTableHandle() {
    return injector.getInstance(OVCTableHandle.class);
  }

}
