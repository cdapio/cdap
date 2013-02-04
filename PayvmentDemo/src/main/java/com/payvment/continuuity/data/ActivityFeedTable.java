package com.payvment.continuuity.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.continuuity.api.data.DataLib;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.data.Write;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;

/**
 * Activity Feed Table implemented as a DataLib/DataSet.
 */
public class ActivityFeedTable extends DataLib {

  public static final String ACTIVITY_FEED_TABLE = "ActivityFeedTable";

  public ActivityFeedTable() {
    super(ACTIVITY_FEED_TABLE, ACTIVITY_FEED_TABLE);
  }

  /**
   * Writes the specified activity feed entry to the activity feed of the
   * specified country and category.
   * @param country
   * @param category
   * @param feedEntry
   */
  public void writeEntry(String country, String category,
      ActivityFeedEntry feedEntry) {
    Write feedEntryWrite = new Write(ACTIVITY_FEED_TABLE,
        makeActivityFeedRow(country, category), feedEntry.getColumn(),
        feedEntry.getValue());
    getCollector().add(feedEntryWrite);
  }
  
  /**
   * Reads the activity feed for the specified category, from times between the
   * maximum and minimum stamps, and up to the specified limit.
   * @param country
   * @param category
   * @param limit maximum number of entries to return
   * @param maxStamp maximum stamp, exclusive
   * @param minStamp minimum stamp, exclusive
   * @return list of feed entries
   * @throws OperationException 
   * @throws IllegalStateException 
   */
  public List<ActivityFeedEntry> readEntries(String country, String category,
      int limit, long maxStamp, long minStamp)
          throws OperationException {
    // ReadColumnRange start is inclusive but we want exclusive, so if the start
    // is non-zero, subtract one
    long exclusiveStamp = Helpers.reverse(maxStamp);
    if (exclusiveStamp != 0) exclusiveStamp--;
    byte [] startColumn = Bytes.toBytes(exclusiveStamp);
    byte [] stopColumn = Bytes.toBytes(Helpers.reverse(minStamp));
    // Perform read from startColumn to stopColumn
    ReadColumnRange read = new ReadColumnRange(ACTIVITY_FEED_TABLE,
        makeActivityFeedRow(country, category), startColumn, stopColumn, limit);
    OperationResult<Map<byte[],byte[]>> result = getDataFabric().read(read);
    List<ActivityFeedEntry> entries = new ArrayList<ActivityFeedEntry>();
    if (!result.isEmpty()) {
      for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
        entries.add(new ActivityFeedEntry(entry.getKey(), entry.getValue()));
      }
    }
    return entries;
  }

  private static final byte [] SEP = new byte [] { ':' };

  public static byte [] makeActivityFeedRow(String country, String category) {
    return Bytes.add(Bytes.toBytes(country), SEP, Bytes.toBytes(category));
  }
}
