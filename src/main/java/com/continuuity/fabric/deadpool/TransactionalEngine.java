/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.deadpool;

import com.continuuity.data.engine.VersionedTable;

/**
 * 
 */
public interface TransactionalEngine extends Engine {

  public VersionedTable getTable(byte [] tableName);

}
