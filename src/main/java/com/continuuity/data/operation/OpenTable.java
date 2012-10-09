package com.continuuity.data.operation;

import com.continuuity.api.data.Operation;
import com.continuuity.api.data.OperationBase;

public class OpenTable implements Operation {

  /** Unique id for the operation */
  private final long id;

  /** name of the table to open */
  final String table;

  @Override
  public long getId() {
    return id;
  }

  /**
   * To open the named table
   * @param tableName the name of the table to open
   */
  public OpenTable(String tableName) {
    this(OperationBase.getId(), tableName);
  }

  /**
   * To open the named table
   * @param id explicit unique id of this operation
   * @param tableName the name of the table to open
   */
  public OpenTable(long id, String tableName) {
    this.id = id;
    this.table = tableName;
  }

  public String getTableName() {
    return table;
  }

  public String toString() {
    return "open(\"" + table + "\")";
  }
}
