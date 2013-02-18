/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.app.program.Store;
import com.continuuity.app.program.StoreFactory;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.Inject;

/**
 *
 */
public class SimpleStoreFactory implements StoreFactory {
  @Override @Inject
  public Store create(MetaDataStore store, MetadataService.Iface mdsService) {
    return new MDSBasedStore(store, mdsService);
  }
}
