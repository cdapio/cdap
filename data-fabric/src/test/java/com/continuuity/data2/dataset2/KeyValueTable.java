package com.continuuity.data2.dataset2;

import com.continuuity.api.dataset.Dataset;

/**
 *
 */
public interface KeyValueTable extends Dataset {
  void put(String key, String value) throws Exception;
  String get(String key) throws Exception;
}
