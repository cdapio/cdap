/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.transactions;

import com.continuuity.fabric.engine.Engine;
import com.continuuity.fabric.engine.table.VersionedTable;

/**
 * 
 */
public interface TransactionalEngine extends Engine {

  public VersionedTable getTable(byte [] tableName);

}
