package com.continuuity.api.data.dataset.table;

import com.continuuity.api.annotation.Beta;

/**
 * Table that keeps all data in memory.
 * One of the usage examples is an in-memory cache. Writes/updates made to this table provide transactional guarantees
 * when used e.g. in flowlet: updates made in process method that failed has no affect.
 */
@Beta
public class MemoryTable extends Table {
  public MemoryTable(String name) {
    super(name);
  }
}
