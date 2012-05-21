package com.continuuity.data.engine;

public interface SQLTable {

  public void startTransaction();
  
  public boolean execute(String sqlStatement);
  
  public void commitTransaction();

}
