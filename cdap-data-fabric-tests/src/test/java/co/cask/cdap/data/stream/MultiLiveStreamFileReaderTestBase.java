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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class MultiLiveStreamFileReaderTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected abstract LocationFactory getLocationFactory();

  @Test
  public void testMultiFileReader() throws Exception {
    String streamName = "multiReader";
    Location location = getLocationFactory().create(streamName);
    location.mkdirs();

    // Create a stream with 1 partition.
    StreamConfig config = new StreamConfig(streamName, Long.MAX_VALUE, 10000, Long.MAX_VALUE, location, null);

    // Write out 200 events in 5 files, with interleaving timestamps
    List<FileWriter<StreamEvent>> writers = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      FileWriter<StreamEvent> writer = createWriter(config, "bucket" + i);

      writers.add(writer);
      for (int j = 0; j < 200; j++) {
        long timestamp = j * 5 + i;
        writer.append(StreamFileTestUtils.createEvent(timestamp, "Testing " + timestamp));
      }
    }

    // Flush all writers.
    for (FileWriter<StreamEvent> writer : writers) {
      writer.flush();
    }

    // Create a multi stream file reader
    List<StreamFileOffset> sources = Lists.newArrayList();
    Location partitionLocation = StreamUtils.createPartitionLocation(config.getLocation(), 0, Long.MAX_VALUE);
    for (int i = 0; i < 5; i++) {
      Location eventFile = StreamUtils.createStreamLocation(partitionLocation, "bucket" + i, 0, StreamFileType.EVENT);
      sources.add(new StreamFileOffset(eventFile, 0L, 0));
    }

    // Reads all events written so far.
    MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(config, sources);
    List<StreamEvent> events = Lists.newArrayList();
    long expectedTimestamp = 0L;
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(100, reader.read(events, 100, 0, TimeUnit.SECONDS));
      Assert.assertEquals(100, events.size());

      for (StreamEvent event : events) {
        Assert.assertEquals(expectedTimestamp, event.getTimestamp());
        Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
        expectedTimestamp++;
      }
      events.clear();
    }

    Assert.assertEquals(0, reader.read(events, 1, 1, TimeUnit.SECONDS));

    // Writes some more events to the first three writers.
    for (int i = 0; i < 3; i++) {
      FileWriter<StreamEvent> writer = writers.get(i);
      for (int j = 0; j < 10; j++) {
        long timestamp = 1000 + j * 3 + i;
        writer.append(StreamFileTestUtils.createEvent(timestamp, "Testing " + timestamp));
      }
    }

    // Close all writers
    for (FileWriter<StreamEvent> writer : writers) {
      writer.close();
    }

    // Continue to read
    Assert.assertEquals(30, reader.read(events, 30, 2, TimeUnit.SECONDS));
    Assert.assertEquals(30, events.size());
    for (StreamEvent event : events) {
      Assert.assertEquals(expectedTimestamp, event.getTimestamp());
      Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
      expectedTimestamp++;
    }

    // Should get no more events.
    Assert.assertEquals(0, reader.read(events, 1, 1, TimeUnit.SECONDS));
    reader.close();
  }

  @Test
  public void testOffsets() throws Exception {
    String streamName = "offsets";
    Location location = getLocationFactory().create(streamName);
    location.mkdirs();

    // Create a stream with 1 partition.
    StreamConfig config = new StreamConfig(streamName, Long.MAX_VALUE, 10000, Long.MAX_VALUE, location, null);

    // Write out 200 events in 5 files, with interleaving timestamps
    for (int i = 0; i < 5; i++) {
      FileWriter<StreamEvent> writer = createWriter(config, "bucket" + i);
      for (int j = 0; j < 200; j++) {
        long timestamp = j * 5 + i;
        writer.append(StreamFileTestUtils.createEvent(timestamp, "Testing " + timestamp));
      }
      writer.close();
    }

    // Create a multi reader
    List<StreamFileOffset> sources = Lists.newArrayList();
    Location partitionLocation = StreamUtils.createPartitionLocation(config.getLocation(), 0, Long.MAX_VALUE);
    for (int i = 0; i < 5; i++) {
      Location eventFile = StreamUtils.createStreamLocation(partitionLocation, "bucket" + i, 0, StreamFileType.EVENT);
      sources.add(new StreamFileOffset(eventFile, 0L, 0));
    }
    MultiLiveStreamFileReader reader = new MultiLiveStreamFileReader(config, sources);

    // Reads some events
    List<StreamEvent> events = Lists.newArrayList();
    long expectedTimestamp = 0L;

    // Read 250 events, in batch size of 10.
    for (int i = 0; i < 25; i++) {
      Assert.assertEquals(10, reader.read(events, 10, 0, TimeUnit.SECONDS));
      Assert.assertEquals(10, events.size());
      for (StreamEvent event : events) {
        Assert.assertEquals(expectedTimestamp, event.getTimestamp());
        Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
        expectedTimestamp++;
      }
      events.clear();
    }

    // Capture the offsets
    Iterable<StreamFileOffset> offsets = ImmutableList.copyOf(
      Iterables.transform(reader.getPosition(), new Function<StreamFileOffset, StreamFileOffset>() {
      @Override
      public StreamFileOffset apply(StreamFileOffset input) {
        return new StreamFileOffset(input);
      }
    }));
    reader.close();

    // Create another multi reader with the offsets
    sources.clear();
    for (StreamFileOffset offset : offsets) {
      sources.add(offset);
    }

    // Read 750 events, in batch size of 10.
    reader = new MultiLiveStreamFileReader(config, sources);
    for (int i = 0; i < 75; i++) {
      Assert.assertEquals(10, reader.read(events, 10, 0, TimeUnit.SECONDS));
      Assert.assertEquals(10, events.size());
      for (StreamEvent event : events) {
        Assert.assertEquals(expectedTimestamp, event.getTimestamp());
        Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
        expectedTimestamp++;
      }
      events.clear();
    }

    Assert.assertEquals(0, reader.read(events, 10, 2, TimeUnit.SECONDS));

    reader.close();
  }

  private FileWriter<StreamEvent> createWriter(StreamConfig config, String prefix) {
    return new TimePartitionedStreamFileWriter(config.getLocation(), config.getPartitionDuration(),
                                               prefix, config.getIndexInterval());
  }
}
