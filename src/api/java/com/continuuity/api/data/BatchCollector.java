package com.continuuity.api.data;

/**
 * This interface is an abstraction for an agent that collects write
 * operations. Eventually all writes added to the batch will be
 * processed in a transactional way. How and when that happens, differs
 * with every implementation of this interface.
 */
public interface BatchCollector {

  /** add an operation to the batch of writes being collected */
  void add(WriteOperation write);

}
