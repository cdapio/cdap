package com.continuuity.data.operation;

/**
 * Clears table data
 */
public class TruncateTable extends Operation {

  // name of the table to truncated
  final String table;

  /**
   * To truncate the named table.
   * @param tableName the name of the table to truncate
   */
  public TruncateTable(String tableName) {
    this.table = tableName;
  }

  /**
   * To truncate the named table.
   * @param id explicit unique id of this operation
   * @param tableName the name of the table to truncate
   */
  public TruncateTable(long id, String tableName) {
    super(id);
    this.table = tableName;
  }

  /**
   * Get the table name.
   * @return the table name
   */
  public String getTableName() {
    return table;
  }

  @Override
  public String toString() {
    return "truncate(\"" + table + "\")";
  }
}
