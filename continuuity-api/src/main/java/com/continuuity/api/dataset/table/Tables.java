package com.continuuity.api.dataset.table;

import com.continuuity.api.app.ApplicationConfigurer;
import com.continuuity.api.dataset.DatasetProperties;

/**
 * Utility for describing {@link Table} and derived data sets within application configuration.
 */
public final class Tables {
  private Tables() {}

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   */
  public static void createTable(ApplicationConfigurer configurer, String datasetName) {
    createTable(configurer, datasetName, ConflictDetection.ROW, -1, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param ttl time to live for data written into a table, in ms. Negative means unlimited
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
    createTable(configurer, datasetName, level, -1, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link Table} data set to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName data set name
   * @param level level on which to detect conflicts in changes made by different transactions
   * @param ttl time to live for data written into a table, in ms. Negative means unlimited
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
   * @param ttl time to live for data written into a table, in ms. Negative means unlimited
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
   * @param ttl time to live for data written into a table, in ms. Negative means unlimited
   * @return {@link DatasetProperties} for the data set
   */
  public static DatasetProperties tableProperties(ConflictDetection level, int ttl, DatasetProperties props) {
    return DatasetProperties.builder()
      .add("conflict.level", level.name())
      .add("ttl", ttl)
      .addAll(props.getProperties())
      .build();

  }
}
