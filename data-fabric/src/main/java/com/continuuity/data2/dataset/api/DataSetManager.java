package com.continuuity.data2.dataset.api;

/**
 *
 */
//todo: define nice exception class?
public interface DataSetManager {
  boolean exists(String name) throws Exception;
  void create(String name) throws Exception;
  void truncate(String name) throws Exception;
  void drop(String name) throws Exception;
}
