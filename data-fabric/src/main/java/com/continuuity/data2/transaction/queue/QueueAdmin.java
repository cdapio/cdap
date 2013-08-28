package com.continuuity.data2.transaction.queue;

import com.continuuity.data2.dataset.api.DataSetManager;

/**
 *
 */
public interface QueueAdmin extends DataSetManager {
  void dropAll() throws Exception;
}
