package com.continuuity.data;

import com.continuuity.data2.dataset.api.DataSetManager;

/**
 *
 */
public interface DataSetAccessor {
  <T> T getDataSetClient(String name, Class<? extends T> type) throws Exception;
  <T> DataSetManager getDataSetManager(Class<? extends T> type) throws Exception;
}
