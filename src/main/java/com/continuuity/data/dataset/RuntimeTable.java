package com.continuuity.data.dataset;

import com.continuuity.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Table;

/**
 * Base class for runtime implementations of Table.
 */
public abstract class RuntimeTable extends Table {

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor
   * @param table the original table
   * @param fabric the data fabric
   */
  RuntimeTable(Table table, DataFabric fabric) {
    super(table.getName());
    this.dataFabric = fabric;
  }

  /**
   * @return the data fabric
   */
  protected DataFabric getDataFabric() {
    return dataFabric;
  }

  /** the data fabric to use for executing synchronous operations */
  private DataFabric dataFabric;

  /**
   * open the table in the data fabric, to ensure it exists and is accessible.
   * @throws com.continuuity.api.data.OperationException if something goes wrong
   */
  public void open() throws OperationException {
    this.dataFabric.openTable(this.getName());
  }

}
