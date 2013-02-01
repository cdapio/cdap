package com.continuuity.data;

import com.continuuity.data.operation.WriteOperation;

/**
 * This interface is an abstraction for an agent that collects write
 * operations. Eventually all writes added to the batch will be
 * processed in a transactional way. How and when that happens, differs
 * with every implementation of this interface.
 */
public interface BatchCollector {

  /**
   * Add an operation to the batch of writes being collected.
   * @param write {@link com.continuuity.data.operation.WriteOperation}
   */
  void add(WriteOperation write);

}
