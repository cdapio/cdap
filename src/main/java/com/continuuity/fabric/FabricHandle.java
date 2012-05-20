/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.fabric.engine.table.VersionedTable;
import com.continuuity.fabric.engine.transactions.TransactionalEngine;

/**
 * 
 */
public class FabricHandle {

  final String username;
  final TransactionalEngine engine;
  
  public FabricHandle(String username, TransactionalEngine engine) {
    this.username = username;
    this.engine = engine;
  }
  
  public VersionedTable getPrimaryTable() {
    return engine.getTable(Bytes.toBytes("__USER_" + username));
  }
}
