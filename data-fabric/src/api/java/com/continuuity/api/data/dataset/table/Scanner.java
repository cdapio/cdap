package com.continuuity.api.data.dataset.table;

/**
 * Interface for table scan operation.
 */
public interface Scanner {

  public Row next();

  public void close();
}
