/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.dataset.DatasetProperties;

/**
 * Utility for describing {@link Table} and derived data sets within application configuration.
 */
public final class Tables {
  private Tables() {}

  /**
   * Indicates that a Table has no (that is, unlimited) time-to-live (TTL).
   */
  public static final int NO_TTL = -1;

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName) {
    createTable(configurer, datasetName, ConflictDetection.ROW, NO_TTL, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param ttl time to live for data written into a table, in ms. {@link #NO_TTL} means unlimited
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName, int ttl) {
    createTable(configurer, datasetName, ConflictDetection.ROW, ttl, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param level level on which to detect conflicts in changes made by different transactions
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName, ConflictDetection level) {
    createTable(configurer, datasetName, level, NO_TTL, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param level level on which to detect conflicts in changes made by different transactions
   * @param ttl time to live for data written into a table, in ms. {@link #NO_TTL} means unlimited
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName,
                                 ConflictDetection level, int ttl) {
    createTable(configurer, datasetName, level, ttl, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param level level on which to detect conflicts in changes made by different transactions
   * @param ttl time to live for data written into a table, in ms. {@link #NO_TTL} means unlimited
   * @param props any additional data set properties
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName,
                                 ConflictDetection level, int ttl,
                                 DatasetProperties props) {

    configurer.createDataset(datasetName, Table.class, tableProperties(level, ttl, props));
  }

  /**
   * Creates properties for {@link Table} or {@link Table} data set instance.
   * @param level level on which to detect conflicts in changes made by different transactions
   * @param ttl time to live for data written into a table, in ms. {@link #NO_TTL} means unlimited
   * @return {@link DatasetProperties} for the data set
   */
  public static DatasetProperties tableProperties(ConflictDetection level, int ttl, DatasetProperties props) {
    return DatasetProperties.builder()
      .add("conflict.level", level.name())
      .add(OrderedTable.PROPERTY_TTL, ttl)
      .addAll(props.getProperties())
      .build();
  }

}
