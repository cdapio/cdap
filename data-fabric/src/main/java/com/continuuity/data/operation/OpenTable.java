package com.continuuity.data.operation;

/**
 * This operations accesses a table without performing any data operation. Itis used to ensure that the table exists
 * and can be accessed.
 */
public class OpenTable extends Operation {

  // name of the table to open
  final String table;

  /**
   * To open the named table.
   * @param tableName the name of the table to open
   */
  public OpenTable(String tableName) {
    this.table = tableName;
  }

  /**
   * To open the named table.
   * @param id explicit unique id of this operation
   * @param tableName the name of the table to open
   */
  public OpenTable(long id, String tableName) {
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
    return "open(\"" + table + "\")";
  }
}
