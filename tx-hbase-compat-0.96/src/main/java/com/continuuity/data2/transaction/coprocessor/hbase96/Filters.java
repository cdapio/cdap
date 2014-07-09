package com.continuuity.data2.transaction.coprocessor.hbase96;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

/**
 *
 */
public final class Filters {
  /**
   * Adds {@code overrideFilter} on to {@code baseFilter}, if it exists, otherwise replaces it.
   */
  public static Filter combine(Filter overrideFilter, Filter baseFilter) {
    if (baseFilter != null) {
      FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filterList.addFilter(baseFilter);
      filterList.addFilter(overrideFilter);
      return filterList;
    }
    return overrideFilter;
  }
}
