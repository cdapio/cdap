/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data;

import com.continuuity.data.table.OrderedVersionedColumnarTable;

/**
 * 
 */
public class FabricHandle {

  final String username;
  
  public FabricHandle(String username) {
    this.username = username;
  }
  
  public OrderedVersionedColumnarTable getPrimaryTable() {
    return null;//getTable(Bytes.toBytes("__USER_" + username));
  }
}
