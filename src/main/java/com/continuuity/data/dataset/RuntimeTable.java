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

  /** the data fabric to use for executing synchronous operations */
  private DataFabric dataFabric;

  /** the name to use for metrics collection, typically the name of the enclosing dataset */
  private String metricName;

  /**
   * @return the data fabric
   */
  protected DataFabric getDataFabric() {
    return dataFabric;
  }

  /**
   * @return the name to use for metrics
   */
  protected String getMetricName() {
    return metricName;
  }

  /**
   * set the name to use for metrics
   * @param metricName the name to use for emitting metrics
   */
  protected void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  /**
   * open the table in the data fabric, to ensure it exists and is accessible.
   * @throws com.continuuity.api.data.OperationException if something goes wrong
   */
  public void open() throws OperationException {
    this.dataFabric.openTable(this.getName());
  }

}
