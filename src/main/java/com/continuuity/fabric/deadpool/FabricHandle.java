/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.deadpool;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.VersionedTable;

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
