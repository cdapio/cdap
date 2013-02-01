package com.continuuity.data.lib;

import com.continuuity.data.operation.ReadOperation;
import com.continuuity.data.operation.WriteOperation;

/**
 * A higher-level data abstraction that converts into {@link com.continuuity.data.operation.Operation}s.
 *
 * A given Library will map to one or more {@link WriteOperation}s and will
 * have an according {@link com.continuuity.data.operation.ReadOperation}.
 */
public interface Library {

  @SuppressWarnings("rawtypes")
  public ReadOperation getRead();

  public WriteOperation[] getWrites();

}
