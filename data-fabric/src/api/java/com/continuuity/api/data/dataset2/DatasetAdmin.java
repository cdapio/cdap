package com.continuuity.api.data.dataset2;

/**
 * Defines the minimum administrative operations a dataset should support.
 *
 * There are no strong strict requirements on what is expected from each operation. Every dataset implementation figures
 * out what is the best for itself.
 *
 * NOTE: even though seems to be not required, the list of common operations helps to bring better structure to dataset
 *       administration design and better guide the design of new datasets.
 *
 * NOTE: the interface does NOT extend {@link java.io.Closeable}: every operation (method) should not left behind held
 *       resources of any kind, like connections to databases, etc.
 */
public interface DatasetAdmin {
  boolean exists() throws Exception;
  void create() throws Exception;
  void drop() throws Exception;
  void truncate() throws Exception;
}
