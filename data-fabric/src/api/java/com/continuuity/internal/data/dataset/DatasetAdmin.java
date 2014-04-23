package com.continuuity.internal.data.dataset;

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
public interface DatasetAdmin extends Closeable {
  boolean exists() throws IOException;
  void create() throws IOException;
  void drop() throws IOException;
  void truncate() throws IOException;
  void upgrade() throws IOException;
}
