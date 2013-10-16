package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.transaction.queue.StreamAdmin;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For now, this implements both QueueAdmin and StreamAdmin, that is, we will bind two instances of this class with
 * different table namespace.
 */
public class LevelDBStreamAdmin extends LevelDBQueueAdmin implements StreamAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBStreamAdmin.class);

  @Inject
  public LevelDBStreamAdmin(DataSetAccessor dataSetAccessor, LevelDBOcTableService service) {
    super(dataSetAccessor, service, "stream");
  }
}
