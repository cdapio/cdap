/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
