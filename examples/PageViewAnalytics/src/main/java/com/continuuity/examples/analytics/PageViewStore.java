/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.analytics;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.Increment;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;

import java.util.HashMap;
import java.util.Map;

/**
 *  A custom-defined DataSet is used to track page views.
 */
public class PageViewStore extends AbstractDataset {

  // Define the underlying table
  private Table table;

  public PageViewStore(DatasetSpecification spec, @EmbeddedDataset("tracks") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  /**
   * Increment the count of a PageView by 1
   * @param pageView a PageView instance
   */
  public void incrementCount(PageView pageView) {
    table.increment(new Increment(pageView.getReferrer(), pageView.getUri(), 1L));
  }

  /**
   * Get the count of requested pages viewed from a specified referrer page
   * @param referrer a URI of the specified referrer page
   * @return a map of a requested page URI to its count
   */
  public Map<String, Long> getPageCount(String referrer) {
    Row row = this.table.get(Bytes.toBytes(referrer));
    if (row == null || row.isEmpty()) {
      return null;
    }
    Map<String, Long> pageCount = new HashMap<String, Long>();
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      pageCount.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return pageCount;
  }

  /**
   * Get the total number of requested pages viewed from a specified referrer page.
   * @param referrer a URI of the specified referrer page
   * @return the number of requested pages
   */
  public long getCounts(String referrer) {
    Row row = this.table.get(Bytes.toBytes(referrer));
    if (row == null || row.isEmpty()) {
      return 0;
    }
    int count = 0;
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      count += Bytes.toLong(entry.getValue());
    }
    return count;
  }
}
