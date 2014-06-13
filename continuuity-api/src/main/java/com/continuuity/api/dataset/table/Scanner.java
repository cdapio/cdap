package com.continuuity.api.dataset.table;

/**
 * Interface for table scan operation.
 */
public interface Scanner {

  public Row next();

  public void close();

}
