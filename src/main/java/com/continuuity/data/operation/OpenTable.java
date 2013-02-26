package com.continuuity.data.operation;

public class OpenTable extends Operation {

  /** name of the table to open */
  final String table;

  /**
   * To open the named table
   * @param tableName the name of the table to open
   */
  public OpenTable(String tableName) {
    this.table = tableName;
  }

  /**
   * To open the named table
   * @param id explicit unique id of this operation
   * @param tableName the name of the table to open
   */
  public OpenTable(long id, String tableName) {
    super(id);
    this.table = tableName;
  }

  public String getTableName() {
    return table;
  }

  public String toString() {
    return "open(\"" + table + "\")";
  }
}
