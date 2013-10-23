package com.continuuity.data.dataset;

import com.continuuity.api.data.dataset.FileDataSet;
import com.continuuity.data.DataFabric;

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
  private final String metricName;

  /**
   * package-protected constructor, only to be called from @see #setReadOnlyTable()
   * and @see ReadWriteTable constructor.
   * @param metricName the name to use for emitting metrics
   * @param fabric the data fabric
   */
  RuntimeFileDataSet(DataFabric fabric, String metricName) {
    super(null, null);
    this.dataFabric = fabric;
    this.metricName = metricName;
  }

  /**
   * @return the name to use for metrics
   */
  protected String getMetricName() {
    return metricName;
  }

  @Override
  public boolean exists() throws IOException {
    return dataFabric.getLocation(getPath()).exists();
  }

  @Override
  public boolean delete() throws IOException {
    return dataFabric.getLocation(getPath()).delete();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return dataFabric.getLocation(getPath()).getInputStream();
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return dataFabric.getLocation(getPath()).getOutputStream();
  }
}
