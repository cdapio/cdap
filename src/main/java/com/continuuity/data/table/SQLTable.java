package com.continuuity.data.table;

public interface SQLTable {

  public void startTransaction();
  
  public boolean execute(String sqlStatement);
  
  public void commitTransaction();

}
