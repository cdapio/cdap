package com.continuuity.common.conflake;

import com.google.common.collect.ImmutableList;

/**
 *
 */
class ConflakeResponse {
  private final ImmutableList<Long> ids;

  public ConflakeResponse(final ImmutableList<Long> ids) {
    this.ids = ids;
  }

  public int getCount() {
    return ids.size();
  }

  public ImmutableList<Long> getIds() {
    return this.ids;
  }
}
