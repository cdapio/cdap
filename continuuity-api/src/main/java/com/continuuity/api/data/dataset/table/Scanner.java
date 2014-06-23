/*
 * Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data.dataset.table;

/**
 * Interface for a table scan operation.
 *
 * @deprecated As of Reactor 2.3.0, replaced by {@link com.continuuity.api.dataset.table.Scanner}
 */
@Deprecated
public interface Scanner {

  public Row next();

  public void close();
}
