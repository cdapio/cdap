package com.continuuity.data2.dataset2.lib.leveldb;

import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;

/**
 * Implementation of this interface needs to know about LevelDB service.
 */
public interface LevelDBAware {
  /**
   * Sets {@link com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService}.
   * @param service instance of {@link com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService}
   */
  void setLevelDBService(LevelDBOcTableService service);
}
