package com.continuuity.data.dataset;

import com.continuuity.api.data.dataset.FileDataSet;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.data.DataFabric;
import com.continuuity.data.operation.executor.TransactionProxy;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public class RuntimeFileDataSet extends FileDataSet {

  // the data fabric to use for executing synchronous operations.
  private final DataFabric dataFabric;

  // the name to use for metrics collection, typically the name of the enclosing dataset
  private String metricName;

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor.
   * @param table the original table
   * @param fabric the data fabric
   */
  RuntimeFileDataSet(FileDataSet table, DataFabric fabric) {
    super(table.getName(), table.getPath());
    this.dataFabric = fabric;
  }

  /**
   * Given a {@link FileDataSet}, create a new {@link RuntimeFileDataSet} and make it the delegate for that
   * dataset.
   *
   * @param dataSet the original dataset
   * @param fabric the data fabric
   * @param metricName the name to use for emitting metrics
   * @return the new {@link FileDataSet}
   */
  public static FileDataSet setRuntimeFileDataSet(FileDataSet dataSet, DataFabric fabric,
                                               String metricName) {
    RuntimeFileDataSet runtimeFileDataSet = new RuntimeFileDataSet(dataSet, fabric);
    runtimeFileDataSet.setMetricName(metricName);
    dataSet.setDelegate(runtimeFileDataSet);
    return runtimeFileDataSet;
  }

  @Override
  public void setDelegate(FileDataSet delegate) {
    // this should never be called - it should only be called on the base class
    throw new UnsupportedOperationException("setDelegate() must not be called on the delegate itself.");
  }


  /**
   * @return the name to use for metrics
   */
  protected String getMetricName() {
    return metricName;
  }

  /**
   * Set the name to use for metrics.
   * @param metricName the name to use for emitting metrics
   */
  protected void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return dataFabric.getInputStream(getPath());
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return dataFabric.getOutputStream(getPath());
  }
}
