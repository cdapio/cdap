package com.continuuity.data.lib;

import com.continuuity.api.data.Operation;
import com.continuuity.api.data.ReadOperation;
import com.continuuity.api.data.WriteOperation;

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
