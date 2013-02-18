/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.metadata.thrift.MetadataService;

/**
 *
 */
public interface StoreFactory {
  Store create(MetaDataStore store, MetadataService.Iface mdsService);
}
