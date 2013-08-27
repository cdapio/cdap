package com.continuuity.data;

import com.continuuity.data2.dataset.api.DataSetManager;

/**
 *
 */
public interface DataSetAccessor {
  <T> T getDataSetClient(String name, Class<? extends T> type) throws Exception;
  DataSetManager getDataSetManager(Class<?> type) throws Exception;
}
