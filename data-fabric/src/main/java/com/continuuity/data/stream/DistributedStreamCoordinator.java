/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.PropertyStore;
import com.continuuity.common.io.Codec;
import com.continuuity.common.zookeeper.store.ZKPropertyStore;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.zookeeper.ZKClient;

/**
 * A {@link StreamCoordinator} uses ZooKeeper to implementation coordination needed for stream.
 */
@Singleton
public final class DistributedStreamCoordinator extends AbstractStreamCoordinator {

  private ZKClient zkClient;

  @Inject
  protected DistributedStreamCoordinator(StreamAdmin streamAdmin) {
    super(streamAdmin);
  }

  @Inject(optional = true)
  void setZkClient(ZKClient zkClient) {
    // Use optional injection for zk client to make testing easier in case this class is not used.
    this.zkClient = zkClient;
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    Preconditions.checkState(zkClient != null, "Missing ZKClient. Check Guice binding.");
    return ZKPropertyStore.create(zkClient, "/" + Constants.Service.STREAMS + "/properties", codec);
  }
}
