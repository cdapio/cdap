/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tests for {@link com.continuuity.data.stream.TimePartitionedStreamFileWriter}.
 */
public abstract class TimePartitionedStreamTestBase {

  private static final Comparator<Location> LOCATION_COMPARATOR = new Comparator<Location>() {
    @Override
    public int compare(Location o1, Location o2) {
      return o1.toURI().toASCIIString().compareTo(o2.toURI().toASCIIString());
    }
  };

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected abstract LocationFactory getLocationFactory();

  @Test
  public void testTimePartition() throws IOException {
    // Create time partition file of 1 seconds each.
    String streamName = "stream";
    Location streamLocation = getLocationFactory().create(streamName);
    streamLocation.mkdirs();
    TimePartitionedStreamFileWriter writer = new TimePartitionedStreamFileWriter(streamLocation, 1000, "file", 100);

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

    Collections.sort(partitionDirs, LOCATION_COMPARATOR);

    // TODO: Test new sequence ID
  }
}
