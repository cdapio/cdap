package com.continuuity.internal.data.dataset.lib.table;

/**
 * Interface for table scan operation.
 */
public interface Scanner {

  public Row next();

  public void close();

}
