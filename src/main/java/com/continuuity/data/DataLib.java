package com.continuuity.data;

/**
 *
 */
public abstract class DataLib implements BatchCollectionClient {
  /**
   * Instance of DataFabric that is inject into DataLib during
   * registration process.
   */
  private DataFabric dataFabric;

  /**
   * Instance of BatchCollector for
   */
  private BatchCollector collector;

  /**
   * Id of dataset.
   */
  private final String dataSetId;

  /**
   * Type of dataset.
   */
  private final String dataSetType;

  public DataLib(final String dataSetId, final String dataSetType) {
    this.dataSetId = dataSetId;
    this.dataSetType = dataSetType;
  }

  /**
   * @return Instance of Data Fabric object
   */
  protected final DataFabric getDataFabric() throws IllegalStateException  {
    if(dataFabric == null) {
      throw new IllegalStateException("Please register DataLib using DataRegistry");
    }
    return dataFabric;
  }

  /**
   * @return Instance of collector for batching operations to Data Fabric.
   */
  public final BatchCollector getCollector() {
    return collector;
  }

  /**
   * @return dataset id.
   */
  public final String getDataSetId() {
    return dataSetId;
  }

  /**
   * @return dataset type.
   */
  public final String getDataSetType() {
    return dataSetType;
  }

  /**
   * Internal function - callback implementation for setting a new batch
   * collector. This function should be only invoked by the framework.
   *
   * @param collector Instance of new collector to be set.
   */
  @Override
  public final void setCollector(BatchCollector collector) {
    this.collector = collector;
  }
}


