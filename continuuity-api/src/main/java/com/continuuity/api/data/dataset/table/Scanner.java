package com.continuuity.api.data.dataset.table;

/**
 * Interface for a table scan operation.
 *
 * @deprecated use {@link com.continuuity.api.dataset.table.Scanner} instead
 */
@Deprecated
public interface Scanner {

  public Row next();

  public void close();
}
