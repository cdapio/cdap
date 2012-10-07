package com.continuuity.data.operation;

import com.continuuity.api.data.Operation;
import com.continuuity.api.data.OperationBase;

public class OpenTable implements Operation {

  /** Unique id for the operation */
  private final long id = OperationBase.getId();

  /** name of the table to open */
  final String table;

  @Override
  public long getId() {
    return id;
  }

  public OpenTable(String tableName) {
    this.table = tableName;
  }

  public String getTableName() {
    return table;
  }

  public String toString() {
    return "open(\"" + table + "\")";
  }
}
