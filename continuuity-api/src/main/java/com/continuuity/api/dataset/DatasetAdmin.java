package com.continuuity.api.dataset;

import com.continuuity.api.annotation.Beta;

import java.io.Closeable;
import java.io.IOException;

/**
 * Defines the minimum administrative operations a dataset should support.
 *
 * There are no strong strict requirements on what is expected from each operation. Every dataset implementation figures
 * out what is the best for itself.
 *
 * NOTE: even though seems to be not required, the list of common operations helps to bring better structure to dataset
 *       administration design and better guide the design of new datasets.
 */
@Beta
public interface DatasetAdmin extends Closeable {
  /**
   * @return true if dataset exists
   * @throws IOException
   */
  boolean exists() throws IOException;

  /**
   * Creates dataset.
   * @throws IOException
   */
  void create() throws IOException;

  /**
   * Drops dataset.
   * @throws IOException
   */
  void drop() throws IOException;

  /**
   * Deletes all data of the dataset.
   * @throws IOException
   */
  void truncate() throws IOException;

  /**
   * Upgrades dataset.
   * @throws IOException
   */
  void upgrade() throws IOException;
}
