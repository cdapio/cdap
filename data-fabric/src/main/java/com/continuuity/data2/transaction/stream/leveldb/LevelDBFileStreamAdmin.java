/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.stream.AbstractFileStreamAdmin;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A file based {@link com.continuuity.data2.transaction.stream.StreamAdmin} that uses LevelDB for maintaining
 * consumer states information.
 */
public final class LevelDBFileStreamAdmin extends AbstractFileStreamAdmin {

  @Inject
  LevelDBFileStreamAdmin(LocationFactory locationFactory, CConfiguration cConf) {
    super(locationFactory, cConf);
  }
}
