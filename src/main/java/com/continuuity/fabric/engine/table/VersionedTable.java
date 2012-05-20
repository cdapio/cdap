/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.table;

import java.util.Map;

/**
 * 
 */
public interface VersionedTable extends SimpleTable {
  
  public void put(byte [] row, byte [] column, long version, byte [] value);
  
  public void put(byte [] row, byte [][] columns, long [] versions,
      byte [][] values);
  
  public boolean delete(byte [] row, byte [] column, long version);
  
  public byte [][] getAllVersions(byte [] row, byte [] column);
  
  public byte [] get(byte [] row, byte [] column, long maxVersion);

  public Map<byte[],byte[]> get(byte [] row, byte [][] columns,
      long maxVersion);
}
