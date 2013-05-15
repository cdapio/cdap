/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.payvment.continuuity.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.payvment.continuuity.Helpers;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Activity Feed Table implemented as a DataLib/DataSet.
 */
public class ActivityFeedTable extends DataSet {

  public static final String ACTIVITY_FEED_TABLE = "ActivityFeedTable";

  private final Table table;

  public ActivityFeedTable(String name) {
    super(name);
    this.table = new Table("activity_feed_" + name);
  }

  public ActivityFeedTable(DataSetSpecification spec) {
    super(spec);
    this.table = new Table("activity_feed_" + getName());
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).dataset(this.table.configure()).create();
  }

  /**
   * Writes the specified activity feed entry to the activity feed of the
   * specified country and category.
   *
   * @param country
   * @param category
   * @param feedEntry
   */
  public void writeEntry(String country, String category, ActivityFeedEntry feedEntry) throws OperationException {
    Write feedEntryWrite = new Write(makeActivityFeedRow(country, category),
                                     feedEntry.getColumn(),
                                     feedEntry.getValue());

    this.table.write(feedEntryWrite);
  }


  /**
   * Reads the activity feed for the specified category, from times between the
   * maximum and minimum stamps, and up to the specified limit.
   *
   * @param country
   * @param category
   * @param limit    maximum number of entries to return
   * @param maxStamp maximum stamp, exclusive
   * @param minStamp minimum stamp, exclusive
   * @return list of feed entries
   * @throws OperationException
   * @throws IllegalStateException
   */
  public List<ActivityFeedEntry> readEntries(String country,
                                             String category,
                                             int limit,
                                             long maxStamp,
                                             long minStamp) throws OperationException {

    // ReadColumnRange start is inclusive but we want exclusive, so if the start
    // is non-zero, subtract one
    long exclusiveStamp = Helpers.reverse(maxStamp);

    if (exclusiveStamp != 0) {
      exclusiveStamp--;
    }

    byte[] startColumn = Bytes.toBytes(exclusiveStamp);
    byte[] stopColumn = Bytes.toBytes(Helpers.reverse(minStamp));

    Read read = new Read(makeActivityFeedRow(country, category), startColumn, stopColumn, limit);
    OperationResult<Map<byte[], byte[]>> result = this.table.read(read);

    List<ActivityFeedEntry> entries = new ArrayList<ActivityFeedEntry>();

    if (!result.isEmpty()) {
      for (Map.Entry<byte[], byte[]> entry : result.getValue().entrySet()) {
        entries.add(new ActivityFeedEntry(entry.getKey(), entry.getValue()));
      }
    }
    return entries;
  }

  private static final byte[] SEP = new byte[]{':'};

  public static byte[] makeActivityFeedRow(String country, String category) {
    return Bytes.add(Bytes.toBytes(country), SEP, Bytes.toBytes(category));
  }
}
