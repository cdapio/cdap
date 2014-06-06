/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file.filter;

import com.continuuity.data.file.ReadFilter;

/**
 * AND multiple @{link ReadFilter}s.
 */
public final class AndReadFilter extends ReadFilter {
  private ReadFilter[] filters;

  public AndReadFilter(ReadFilter...filters) {
    this.filters = filters;
  }

  @Override
  public boolean acceptOffset(long offset) {
    for (ReadFilter filter : filters) {
      if (!filter.acceptOffset(offset)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean acceptTimestamp(long timestamp) {
    for (ReadFilter filter : filters) {
      if (!filter.acceptTimestamp(timestamp)) {
        return false;
      }
    }
    return true;
  }
}
