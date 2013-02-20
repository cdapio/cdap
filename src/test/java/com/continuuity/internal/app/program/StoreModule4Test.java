/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.app.store.Store;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.internal.app.store.MDSBasedStore;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.AbstractModule;

public class StoreModule4Test extends AbstractModule {
  @Override
  protected void configure() {
    bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
    bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
    bind(Store.class).to(MDSBasedStore.class);
  }
}
