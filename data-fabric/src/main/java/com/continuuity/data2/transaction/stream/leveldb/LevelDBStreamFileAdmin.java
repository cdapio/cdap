/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBStreamAdmin;
import com.continuuity.data2.transaction.stream.AbstractStreamFileAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A file based {@link com.continuuity.data2.transaction.stream.StreamAdmin} that uses LevelDB for maintaining
 * consumer states information.
 */
public final class LevelDBStreamFileAdmin extends AbstractStreamFileAdmin {

  @Inject
  LevelDBStreamFileAdmin(LocationFactory locationFactory, CConfiguration cConf, StreamCoordinator streamCoordinator,
                         StreamConsumerStateStoreFactory stateStoreFactory, LevelDBStreamAdmin oldStreamAdmin) {
    super(locationFactory, cConf, streamCoordinator, stateStoreFactory, oldStreamAdmin);
  }
}
