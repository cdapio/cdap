/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.engine.table;

import java.util.Map;

/**
 * 
 */
public interface SimpleTable {

  public void put(byte [] row, byte [] column, byte [] value);
  
  public void put(byte [] row, byte [][] columns, byte [][] values);
  
  public boolean delete(byte [] row);
  
  public boolean delete(byte [] row, byte [] column);

  public byte [] get(byte [] row, byte [] column);

  public Map<byte[],byte[]> get(byte [] row, byte [][] columns);
}
