package com.continuuity.examples.pageViewAnalytics;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.data.dataset.table.Table;

import java.util.HashMap;
import java.util.Map;

/**
 *  A custom-defined DataSet is used to track page views.
 */
public class PageViewStore extends DataSet {

  // Define the underlying table
  private Table table;

  public PageViewStore(String name) {
    super(name);
    this.table = new Table("tracks");
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
