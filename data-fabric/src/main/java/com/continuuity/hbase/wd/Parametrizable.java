package com.continuuity.hbase.wd;

/**
 * Defines interface for storing object parameters in a String object to later restore the object by applying them to
 * new object instance
 */
public interface Parametrizable {
  String getParamsToStore();
  void init(String storedParams);
}
