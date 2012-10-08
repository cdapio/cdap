package com.continuuity.metrics2.temporaldb.internal;

import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.temporaldb.Query;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 10/7/12
 * Time: 5:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryFilter {
  private final Query query;

  public QueryFilter(Query query) {
    if(query == null){
      throw new IllegalArgumentException("query");
    }
    this.query = query;
  }

  public boolean apply(DataPoint dataPoint) {
    return apply(dataPoint.getMetric(), dataPoint.getTimestamp(),
                 dataPoint.getTags());
  }

  public boolean apply(String metric, long timestamp,
                       Map<String, String> tags) {
    if (metric == null || metric.equals(query.getMetricName()) == false) {
      return false;
    }
    if (timestamp < query.getStartTime() || timestamp > query.getEndTime()) {
      return false;
    }

    Map<String, String> tagFilters = query.getTagFilter();
    if (tagFilters == null || tagFilters.isEmpty()) {
      // no filters specified, so not check is needed
      return true;
    } else {
      for (Entry<String, String> tagFilter : tagFilters.entrySet()) {
        if (applyTagFilter(tagFilter.getKey(), tagFilter.getValue(),
                           tags) == false) {
          return false;
        }
      }
      return true;
    }
  }

  private boolean applyTagFilter(String tagFilterKey, String tagFilterValue,
                                 Map<String, String> tags) {
    if (tags == null || tags.isEmpty()) {
      return true;
    }
    String propertyValue = tags.get(tagFilterKey);
    if (propertyValue == null) {
      // The requested tag is not present
      return false;
    } else if (tagFilterValue.equals(Query.WILDCARD)) {
      // a wildcard is used, the value only needs to be present
      return true;
    } else if (tagFilterValue.equals(propertyValue)) {
      // the tag filter value matches the property value
      return true;
    }
    return false;
  }

}
