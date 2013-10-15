package com.continuuity.api.data.dataset.table;

/**
 * Interface for a table scan operation.
 */
public interface Scanner {

  public Row next();

  public void close();
}
