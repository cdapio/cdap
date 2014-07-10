/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream.leveldb;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data.stream.StreamEventOffset;
import com.continuuity.data.stream.StreamFileOffset;
import com.continuuity.data.stream.StreamFileType;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.leveldb.LevelDBStreamAdmin;
import com.continuuity.data2.transaction.stream.AbstractStreamFileConsumerFactory;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.data2.transaction.stream.StreamConsumer;
import com.continuuity.data2.transaction.stream.StreamConsumerState;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStore;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * A {@link com.continuuity.data2.transaction.stream.StreamConsumerFactory} that reads from stream file
 * and uses LevelDB as the store for consumer process states.
 */
public final class LevelDBStreamFileConsumerFactory extends AbstractStreamFileConsumerFactory {

  private final CConfiguration cConf;
  private final LevelDBOcTableService tableService;
  private final ConcurrentMap<String, Object> dbLocks;

  @Inject
  LevelDBStreamFileConsumerFactory(DataSetAccessor dataSetAccessor, StreamAdmin streamAdmin,
                                   StreamConsumerStateStoreFactory stateStoreFactory,
                                   CConfiguration cConf, LevelDBOcTableService tableService,
                                   QueueClientFactory queueClientFactory, LevelDBStreamAdmin oldStreamAdmin) {
    super(dataSetAccessor, streamAdmin, stateStoreFactory, queueClientFactory, oldStreamAdmin);
    this.cConf = cConf;
    this.tableService = tableService;
    this.dbLocks = Maps.newConcurrentMap();
  }


  @Override
  protected StreamConsumer create(String tableName, StreamConfig streamConfig, ConsumerConfig consumerConfig,
                                  StreamConsumerStateStore stateStore, StreamConsumerState beginConsumerState,
                                  FileReader<StreamEventOffset, Iterable<StreamFileOffset>> reader,
                                  @Nullable ReadFilter extraFilter) throws IOException {

    tableService.ensureTableExists(tableName);

    LevelDBOcTableCore tableCore = new LevelDBOcTableCore(tableName, tableService);
    Object dbLock = getDBLock(tableName);
    return new LevelDBStreamFileConsumer(cConf, streamConfig, consumerConfig, reader,
                                         stateStore, beginConsumerState, extraFilter,
                                         tableCore, dbLock);
  }

  @Override
  protected void dropTable(String tableName) throws IOException {
    tableService.dropTable(tableName);
  }

  @Override
  protected void getFileOffsets(Location partitionLocation,
                                Collection<? super StreamFileOffset> fileOffsets,
                                int generation) throws IOException {
    // Assumption is it's used in local mode, hence only one instance
    Location eventLocation = StreamUtils.createStreamLocation(partitionLocation,
                                                              cConf.get(Constants.Stream.FILE_PREFIX) + ".0",
                                                              0, StreamFileType.EVENT);
    fileOffsets.add(new StreamFileOffset(eventLocation, 0, generation));
  }

  private Object getDBLock(String name) {
    Object lock = dbLocks.get(name);
    if (lock == null) {
      lock = new Object();
      Object existing = dbLocks.putIfAbsent(name, lock);
      if (existing != null) {
        lock = existing;
      }
    }
    return lock;

  }
}
