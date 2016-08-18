/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.security.DefaultImpersonator;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link co.cask.cdap.data.stream.TimePartitionedStreamFileWriter}.
 */
public abstract class TimePartitionedStreamTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected static CConfiguration cConf = CConfiguration.create();

  protected abstract LocationFactory getLocationFactory();

  private static final Impersonator impersonator = new DefaultImpersonator(cConf, new UnsupportedUGIProvider(), null);

  @Test
  public void testTimePartition() throws IOException {
    // Create time partition file of 1 seconds each.
    String streamName = "stream";
    Location streamLocation = getLocationFactory().create(streamName);
    streamLocation.mkdirs();
    TimePartitionedStreamFileWriter writer = new TimePartitionedStreamFileWriter(
      streamLocation, 1000, "file", 100, new StreamId(NamespaceId.DEFAULT.getNamespace(), streamName), impersonator);

    // Write 2 events per millis for 3 seconds, starting at 0.5 second.
    long timeBase = 500;
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 1000; j++) {
        long offset = i * 1000 + j;
        long timestamp = timeBase + offset;
        writer.append(StreamFileTestUtils.createEvent(timestamp, "Testing " + offset + " 0"));
        writer.append(StreamFileTestUtils.createEvent(timestamp, "Testing " + offset + " 1"));
      }
    }

    writer.close();

    // There should be four partition directory (500-1000, 1000-2000, 2000-3000, 3000-3500).
    List<Location> partitionDirs = Lists.newArrayList(streamLocation.list());
    Assert.assertEquals(4, partitionDirs.size());

    // The start time for the partitions should be 0, 1000, 2000, 3000
    Collections.sort(partitionDirs, Locations.LOCATION_COMPARATOR);
    for (int i = 0; i < 4; i++) {
      Assert.assertEquals(i * 1000, StreamUtils.getPartitionStartTime(partitionDirs.get(i).getName()));
    }
  }

  @Test
  public void testAppendAll() throws IOException {
    // Create time partition file of 1 seconds each.
    String streamName = "testAppendAll";
    Location streamLocation = getLocationFactory().create(streamName);
    streamLocation.mkdirs();
    TimePartitionedStreamFileWriter writer = new TimePartitionedStreamFileWriter(
      streamLocation, 1000, "file", 100, new StreamId(NamespaceId.DEFAULT.getNamespace(), streamName), impersonator);

    // Write 2 events per millis for 3 seconds, starting at 0.5 second.
    List<StreamEvent> events = Lists.newArrayList();
    long timeBase = 500;
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 1000; j++) {
        long offset = i * 1000 + j;
        long timestamp = timeBase + offset;
        events.add(StreamFileTestUtils.createEvent(timestamp, "Testing " + offset + " 0"));
        events.add(StreamFileTestUtils.createEvent(timestamp, "Testing " + offset + " 1"));
      }
    }
    writer.appendAll(events.iterator());
    writer.close();

    // There should be four partition directory (500-1000, 1000-2000, 2000-3000, 3000-3500).
    List<Location> partitionDirs = Lists.newArrayList(streamLocation.list());
    Assert.assertEquals(4, partitionDirs.size());

    // The start time for the partitions should be 0, 1000, 2000, 3000
    Collections.sort(partitionDirs, Locations.LOCATION_COMPARATOR);
    for (int i = 0; i < 4; i++) {
      Assert.assertEquals(i * 1000, StreamUtils.getPartitionStartTime(partitionDirs.get(i).getName()));
    }
  }
}
