/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;

import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metadata.MetaDataTable;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 *
 */
public class MDTBasedStoreFactory implements StoreFactory {
  private final MetaDataTable table;
  private final CConfiguration configuration;
  private final LocationFactory lFactory;

  @Inject
  public MDTBasedStoreFactory(CConfiguration configuration,
                              MetaDataTable table,
                              LocationFactory lFactory) {
    this.configuration = configuration;
    this.table = table;
    this.lFactory = lFactory;
  }

  @Override
  public Store create() {
    return new MDTBasedStore(configuration, table, lFactory);
  }
}
