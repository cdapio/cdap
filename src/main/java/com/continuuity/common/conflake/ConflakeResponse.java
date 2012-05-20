package com.continuuity.common.conflake;

import com.google.common.collect.ImmutableList;

/**
 * Conflake Actor response with collection of ids. This class is immutable.
 */
class ConflakeResponse {
  private final ImmutableList<Long> ids;

  /**
   * Creates an instance of ConflakeResponse object with ids to be returned.
   * @param ids
   */
  public ConflakeResponse(final ImmutableList<Long> ids) {
    this.ids = ids;
  }

  /**
   * Returns number of Ids returned in the response.
   * @return number of ids.
   */
  public int getCount() {
    return ids.size();
  }

  /**
   * Returns ids generated.
   * @return generated ids.
   */
  public ImmutableList<Long> getIds() {
    return this.ids;
  }
}
