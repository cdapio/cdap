/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Defines a class to test EntryScanner in TimeseriesTable
 */
public class TimeseriesTableScannerTest extends AbstractDatasetTest {
  private static final Id.DatasetInstance facts = Id.DatasetInstance.from(NAMESPACE_ID, "facts");
  private static final byte[] ALL_KEY = Bytes.toBytes("a");
  private static final String SRC_TAG = "src";
  private static final String DST_TAG = "dst";
  private static final String SRC_DEVICE_ID_TAG = "src_device_id";
  private static final String DEST_DEVICE_ID_TAG = "dest_device_id";

  private static TimeseriesTable table = null;
  @BeforeClass
  public static void setup() throws Exception {
    createInstance("timeseriesTable", facts, DatasetProperties.EMPTY);
    table = getInstance(facts);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteInstance(facts);
  }

  @Test
  public void test() throws Exception {
    long now = System.currentTimeMillis();

    sendData(now);

    // verify
    testScan(now);
    testNoRow(now);
    testFilter(now);
    testNoTagMatch(now);
    testReadEntryWithoutTag(now);
  }

  private void sendData(long now) throws IOException, InterruptedException {
    // send facts in time window (now - 10 mins, now)
    Fact f1 = new Fact(now - TimeUnit.MINUTES.toMillis(1),
                       ImmutableMap.of(SRC_TAG, "10.192.18.0", DST_TAG, "10.123.8.20", SRC_DEVICE_ID_TAG, "device1",
                                       DEST_DEVICE_ID_TAG, "device2"));
    writeFact(f1);

    Fact f2 = new Fact(now - TimeUnit.MINUTES.toMillis(2),
                       ImmutableMap.of(SRC_TAG, "10.192.18.0", DST_TAG, "10.123.8.30", SRC_DEVICE_ID_TAG, "device1",
                                       DEST_DEVICE_ID_TAG, "device2"));
    writeFact(f2);

    Fact f3 = new Fact(now - TimeUnit.MINUTES.toMillis(10),
                       ImmutableMap.of(SRC_TAG, "20.192.18.0", DST_TAG, "10.123.8.40", SRC_DEVICE_ID_TAG, "device1",
                                       DEST_DEVICE_ID_TAG, "device3"));
    writeFact(f3);

    // send facts in time window (now - 20 mins, now - 10 mins)
    Fact f4 = new Fact(now - TimeUnit.MINUTES.toMillis(12),
                       ImmutableMap.of(SRC_TAG, "12.192.18.0", DST_TAG, "20.123.8.50", SRC_DEVICE_ID_TAG, "device2",
                                       DEST_DEVICE_ID_TAG, "device4"));
    writeFact(f4);

    Fact f5 = new Fact(now - TimeUnit.MINUTES.toMillis(15),
                       ImmutableMap.of(SRC_TAG, "12.192.18.0", DST_TAG, "20.123.8.60", SRC_DEVICE_ID_TAG, "device4",
                                       DEST_DEVICE_ID_TAG, "device2"));
    writeFact(f5);

    // f6 and f7 are written to the table without tag.
    Fact f6 = new Fact(now - TimeUnit.SECONDS.toMillis(10),
                       ImmutableMap.of(SRC_TAG, "1.1.1.1", DST_TAG, "20.123.8.60", SRC_DEVICE_ID_TAG, "device4",
                                       DEST_DEVICE_ID_TAG, "device2"));
    writeFact(f6);

    Fact f7 = new Fact(now - TimeUnit.SECONDS.toMillis(20),
                       ImmutableMap.of(SRC_TAG, "1.1.1.1", DST_TAG, "20.123.8.60", SRC_DEVICE_ID_TAG, "device4",
                                       DEST_DEVICE_ID_TAG, "device2"));
    writeFact(f7);
  }

  /**
   * Tests the correctness of facts fetched by EntryScanner.
   */
  private void testScan(long now) {
    long endTs = now;
    long startTs = now - TimeUnit.MINUTES.toMillis(18) + 1;

    // read facts in time window (now - 18mins, now)
    Iterator<TimeseriesTable.Entry> entryIterator = table.read(ALL_KEY, startTs, endTs);

    // dsts of the facts sent from the earliest ts to now
    ImmutableSet<String> expectedDsts = ImmutableSet.of("20.123.8.60", "20.123.8.50", "10.123.8.40", "10.123.8.30",
                                                        "10.123.8.20");
    Set<String> actualDsts = Sets.newHashSet();
    while (entryIterator.hasNext()) {
      TimeseriesTable.Entry entry = entryIterator.next();
      actualDsts.add(Bytes.toString(entry.getValue()));
    }

    Assert.assertEquals(5, expectedDsts.size());
    Assert.assertEquals("dst is not correct", expectedDsts, actualDsts);
  }

  /**
   * Tests no entry returned in the time interval.
   */
  private void testNoRow(long now) {
    long endTs = now + TimeUnit.MINUTES.toMillis(20);
    long startTs = now + TimeUnit.MINUTES.toMillis(10);
    Iterator<TimeseriesTable.Entry> entryIterator = table.read(ALL_KEY, startTs, endTs);
    Assert.assertFalse("No entry returned in the time interval", entryIterator.hasNext());
  }

  /**
   * Tests iterator filter. queryTags will be sorted inside EntryScanner.
   */
  private void testFilter(long now) {
    long endTs = now;
    long startTs = now - TimeUnit.MINUTES.toMicros(37);

    String[] queryTags = {"device2", "device1"};

    Iterator<TimeseriesTable.Entry> entryIterator = table.read(ALL_KEY, startTs, endTs, Bytes.toByteArrays(queryTags));

    List<TimeseriesTable.Entry> result = Lists.newArrayList();
    while (entryIterator.hasNext()) {
      TimeseriesTable.Entry entry = entryIterator.next();
      result.add(entry);
    }
    Assert.assertEquals(2, result.size());
  }

  /**
   * Tests queryTags are not matched.
   */
  private void testNoTagMatch(long now) {
    long endTs = now;
    long startTs = now - TimeUnit.MINUTES.toMicros(37);

    String[] queryTags = {"device5", "device2"};
    Iterator<TimeseriesTable.Entry> entryIterator = table.read(ALL_KEY, startTs, endTs, Bytes.toByteArrays(queryTags));
    Assert.assertFalse(entryIterator.hasNext());
  }

  /**
   * Tests reading entries without tag.
   */
  private void testReadEntryWithoutTag(long now) {
    long endTs = now;
    long startTs = now - TimeUnit.SECONDS.toMillis(30);
    List<TimeseriesTable.Entry> result = Lists.newArrayList();

    // f6 and f7 are read back, since they are written to the table without tag.
    Iterator<TimeseriesTable.Entry> entryIterator = table.read(ALL_KEY, startTs, endTs);
    while (entryIterator.hasNext()) {
      result.add(entryIterator.next());
    }
    Assert.assertEquals(2, result.size());
  }


  private void writeFact(Fact fact) {
    long ts = fact.getTs();
    byte[] srcTag = Bytes.toBytes(fact.getDimensions().get(SRC_DEVICE_ID_TAG));
    byte[] destTag = Bytes.toBytes(fact.getDimensions().get(DEST_DEVICE_ID_TAG));
    if (fact.getDimensions().get(SRC_DEVICE_ID_TAG).equals("1.1.1.1")) {
      table.write(new TimeseriesTable.Entry(ALL_KEY, Bytes.toBytes(fact.getDimensions().get(DST_TAG)), ts));
    } else {
      table.write(new TimeseriesTable.Entry(ALL_KEY, Bytes.toBytes(fact.getDimensions().get(DST_TAG)), ts,
                                            srcTag, destTag));
    }
  }

  private class Fact {
    // map dimension name -> value
    // NOTE: using concrete sorted map implementation here to ease json serde in tests
    private TreeMap<String, String> dimensions;
    private long ts;

    public Fact(long ts, Map<String, String> dimensions) {
      // todo: avoid copying maps
      this.dimensions = Maps.newTreeMap(ImmutableSortedMap.copyOf(dimensions));
      this.ts = ts;
    }

    public Map<String, String> getDimensions() {
      return dimensions;
    }

    public long getTs() {
      return ts;
    }

    // this is handy method for re-using same fact instance
    public void setTs(long ts) {
      this.ts = ts;
    }

    public byte[] buildKey() {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, String> dimVal : dimensions.entrySet()) {
        sb.append(dimVal.getKey().length()).append(dimVal.getKey());
        sb.append(dimVal.getValue().length()).append(dimVal.getValue());
      }
      return Bytes.toBytes(sb.toString());
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(Fact.class)
        .add("ts", ts)
        .add("dimensions", dimensions)
        .toString();
    }
  }
}
