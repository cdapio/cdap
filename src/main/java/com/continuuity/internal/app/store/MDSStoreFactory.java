/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.store;

import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.Inject;

/**
 *
 */
public class MDSStoreFactory implements StoreFactory {
  private final MetaDataStore store;
  private final MetadataService.Iface mdsService;

  @Inject
  public MDSStoreFactory(MetaDataStore store, MetadataService.Iface mdsService) {
    this.store = store;
    this.mdsService = mdsService;
  }

  @Override
  public Store create() {
    return new MDSBasedStore(store, mdsService);
  }
}
