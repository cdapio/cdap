/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file;

import com.continuuity.data.file.filter.AndReadFilter;

/**
 * Utility functions for {@link com.continuuity.data.file.ReadFilter}.
 */
public final class ReadFilters {
  private ReadFilters() {}

  public static final ReadFilter and(ReadFilter...filters) {
    return new AndReadFilter(filters);
  }
}
