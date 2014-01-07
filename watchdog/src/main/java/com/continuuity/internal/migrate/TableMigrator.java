package com.continuuity.internal.migrate;

import com.continuuity.data2.OperationException;

/**
 *
 */
public interface TableMigrator {
  void migrateIfRequired() throws OperationException;
}
