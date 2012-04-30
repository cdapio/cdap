package com.continuuity.fabric.operations.lib;

import com.continuuity.fabric.operations.Operation;
import com.continuuity.fabric.operations.ReadOperation;
import com.continuuity.fabric.operations.WriteOperation;

/**
 * A higher-level data abstraction that converts into {@link Operation}s.
 *
 * A given Library will map to one or more {@link WriteOperation}s and will
 * have an according {@link ReadOperation}.
 */
public interface Library {

  @SuppressWarnings("rawtypes")
  public ReadOperation getRead();

  public WriteOperation [] getWrites();

}
