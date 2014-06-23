package com.continuuity.api.data.dataset.table;

/**
 * Table that keeps all data in memory.
 * One of the usage examples is an in-memory cache. Writes/updates made to this table provide transactional guarantees
 * when used. For example, updates made in a flowlet process method that failed will have no effect.
 * 
 * @deprecated use {@link com.continuuity.api.dataset.table.MemoryTable} instead
 */
@Deprecated
public class MemoryTable extends Table {
  public MemoryTable(String name) {
    super(name);
  }
}
