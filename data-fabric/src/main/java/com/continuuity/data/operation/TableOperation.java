package com.continuuity.data.operation;

/**
 * Defines write operation that operates on single table
 */
public interface TableOperation {
  /**
   * @return table name
   */
  String getTable();
}
