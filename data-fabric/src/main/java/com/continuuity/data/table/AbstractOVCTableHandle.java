package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;

/**
 * Provides common implementations for some OVC TableHandle methods.
 */
public abstract class AbstractOVCTableHandle implements OVCTableHandle {
  /**
   * A configuration object. Not currently used (for real).
   */
  private CConfiguration conf = new CConfiguration();

  @Override
  public abstract OrderedVersionedColumnarTable getTable(byte[] tableName) throws OperationException;

}
