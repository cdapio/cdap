package com.continuuity.api.dataset.lib;

import com.continuuity.api.app.ApplicationConfigurer;
import com.continuuity.api.dataset.DatasetProperties;

/**
 * Utility for describing {@link TimeseriesTable} data set within application configuration.
 */
public final class TimeseriesTables {
  private TimeseriesTables() {}

  /**
   * Adds {@link TimeseriesTable} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName, int timeIntervalToStorePerRow) {
    configurer.createDataset(datasetName, TimeseriesTable.class.getName(),
                             timeseriesTableProperties(timeIntervalToStorePerRow, DatasetProperties.EMPTY));
  }

  /**
   * Creates properties for {@link TimeseriesTable} data set instance.
   * @param timeIntervalToStorePerRow time interval to store per row. See {@link TimeseriesTable} for details.
   * @return {@link com.continuuity.api.dataset.DatasetProperties} for the data set
   */
  public static DatasetProperties timeseriesTableProperties(int timeIntervalToStorePerRow, DatasetProperties props) {
    return DatasetProperties.builder()
      .add(TimeseriesTable.ATTR_TIME_INTERVAL_TO_STORE_PER_ROW, timeIntervalToStorePerRow)
      .addAll(props.getProperties())
      .build();
  }
}
