package com.continuuity.metrics2.temporaldb.internal;


import com.continuuity.metrics2.temporaldb.Query;

/**
 * Key based filter.
 */
class KeyBasedQueryFilter {
  private final int metricID;
  private final long startTimestamp;
  private final long endTimestamp;
  private final int[][] tagFilters;

  public KeyBasedQueryFilter(int metricID, long startTimestamp,
                             long endTimestamp, int[][] tagFilters) {
    this.metricID = metricID;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.tagFilters = tagFilters;
  }

  public boolean apply(int metricID, long timestamp, int[][] tags) {
    if (this.metricID != metricID) {
      return false;
    }
    if (timestamp < startTimestamp || timestamp > endTimestamp) {
      return false;
    }

    if (tagFilters == null || tagFilters.length == 0) {
      // no filters specified, so not check is needed
      return true;
    } else {
      for (int i = 0; i < tagFilters.length; i++) {
        if (applyTagFilter(tagFilters[i][0], tagFilters[i][1], tags) == false) {
          return false;
        }
      }
      return true;
    }
  }

  private int[] getTag(int key, int[][] tags) {
    for (int i = 0; i < tags.length; i++) {
      if (tags[i][0] == key) {
        return tags[i];
      }
    }
    return null;
  }

  private boolean applyTagFilter(int tagFilterKey, int tagFilterValue,
                                 int[][] tags) {
    if (tags == null || tags.length == 0) {
      return false;
    }
    int[] property = getTag(tagFilterKey, tags);
    if (property == null) {
      return false;
    } else {
      int propertyValue = property[1];
      if (tagFilterValue == Query.WILDCARD_ID) {
        // a wildcard is used, the value only needs to be present
        return true;
      } else if (tagFilterValue == propertyValue) {
        return true;
      }
      return false;
    }
  }
}
