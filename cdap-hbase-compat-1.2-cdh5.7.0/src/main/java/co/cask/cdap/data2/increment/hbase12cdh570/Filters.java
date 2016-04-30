/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.increment.hbase12cdh570;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

/**
 * Utility methods for working with HBase filters.
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
