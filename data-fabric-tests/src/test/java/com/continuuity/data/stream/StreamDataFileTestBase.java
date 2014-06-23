/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Locations;
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.test.SlowTests;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test cases for StreamDataFileReader/Writer.
 */
public abstract class StreamDataFileTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamDataFileTestBase.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected abstract LocationFactory getLocationFactory();

  protected abstract StreamAdmin getStreamAdmin();

  /**
   * Test for basic read write to verify data encode/decode correctly.
   * @throws Exception
   */
  @Test
  public void testBasicReadWrite() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           10000L);

    // Write 100 events to the stream, with 20 even timestamps
    for (int i = 0; i < 40; i += 2) {
      for (int j = 0; j < 5; j++) {
        writer.append(StreamFileTestUtils.createEvent(i, "Basic test " + i));
      }
    }

    writer.close();

    // Create a reader that starts from beginning.
    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));
    List<StreamEvent> events = Lists.newArrayList();
    Assert.assertEquals(100, reader.read(events, 100, 1, TimeUnit.SECONDS));
    Assert.assertEquals(-1, reader.read(events, 100, 1, TimeUnit.SECONDS));

    reader.close();

    // Collect the events in a multimap for verification
    Multimap<Long, String> messages = LinkedListMultimap.create();
    for (StreamEvent event : events) {
      messages.put(event.getTimestamp(), Charsets.UTF_8.decode(event.getBody()).toString());
    }

    // 20 timestamps
    Assert.assertEquals(20, messages.keySet().size());
    for (Map.Entry<Long, Collection<String>> entry : messages.asMap().entrySet()) {
      // Each timestamp has 5 messages
      Assert.assertEquals(5, entry.getValue().size());
      // All 5 messages for a timestamp are the same
      Assert.assertEquals(1, ImmutableSet.copyOf(entry.getValue()).size());
      // Message is "Basic test " + timestamp
      Assert.assertEquals("Basic test " + entry.getKey(), entry.getValue().iterator().next());
    }
  }

  @Test
  public void testTail() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    final Location eventFile = dir.getTempFile(".dat");
    final Location indexFile = dir.getTempFile(".idx");

    final CountDownLatch writerStarted = new CountDownLatch(1);
    // Create a thread for writing 10 events, 1 event per 200 milliseconds.
    // It pauses after writing 5 events.
    final CountDownLatch waitLatch = new CountDownLatch(1);
    Thread writerThread = new Thread() {
      @Override
      public void run() {
        try {
          StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                                 Locations.newOutputSupplier(indexFile),
                                                                 10000L);
          writerStarted.countDown();

          for (int i = 0; i < 10; i++) {
            writer.append(StreamFileTestUtils.createEvent(i, "Testing " + i));
            writer.flush();
            TimeUnit.MILLISECONDS.sleep(200);
            if (i == 4) {
              waitLatch.await();
            }
          }
          writer.close();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));
    List<StreamEvent> events = Lists.newArrayList();

    writerThread.start();
    writerStarted.await();

    // Expect 10 events, followed by EOF.
    Assert.assertEquals(5, reader.read(events, 5, 2000, TimeUnit.MILLISECONDS));
    waitLatch.countDown();
    Assert.assertEquals(5, reader.read(events, 5, 2000, TimeUnit.MILLISECONDS));
    Assert.assertEquals(-1, reader.read(events, 1, 500, TimeUnit.MILLISECONDS));

    Assert.assertEquals(10, events.size());
    // Verify the ordering of events
    int ts = 0;
    for (StreamEvent event : events) {
      Assert.assertEquals(ts, event.getTimestamp());
      Assert.assertEquals("Testing " + ts, Charsets.UTF_8.decode(event.getBody()).toString());
      ts++;
    }
  }

  @Test
  public void testIndex() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Write 1000 events with different timestamps, and create index for every 100 timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           100L);
    for (int i = 0; i < 1000; i++) {
      writer.append(StreamFileTestUtils.createEvent(1000 + i, "Testing " + i));
    }
    writer.close();

    // Read with index
    for (long ts : new long[] {1050, 1110, 1200, 1290, 1301, 1400, 1500, 1600, 1898, 1900, 1999}) {
      StreamDataFileReader reader = StreamDataFileReader.createByStartTime(Locations.newInputSupplier(eventFile),
                                                                           Locations.newInputSupplier(indexFile),
                                                                           ts);
      Queue<StreamEvent> events = Lists.newLinkedList();
      Assert.assertEquals(1, reader.read(events, 1, 1L, TimeUnit.MILLISECONDS));
      Assert.assertEquals(ts, events.poll().getTimestamp());

      reader.close();
    }
  }

  @Test
  public void testPosition() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Write 10 events with different timestamps. Index doesn't matter
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           100L);

    for (int i = 0; i < 10; i++) {
      writer.append(StreamFileTestUtils.createEvent(i, "Testing " + i));
    }
    writer.close();

    // Read 4 events
    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));
    List<StreamEvent> events = Lists.newArrayList();
    reader.read(events, 4, 1, TimeUnit.SECONDS);

    Assert.assertEquals(4, events.size());

    for (StreamEvent event : events) {
      Assert.assertEquals("Testing " + event.getTimestamp(), Charsets.UTF_8.decode(event.getBody()).toString());
    }

    long position = reader.getPosition();
    reader.close();

    // Open a new reader, read from the last position.
    reader = StreamDataFileReader.createWithOffset(Locations.newInputSupplier(eventFile),
                                                   Locations.newInputSupplier(indexFile),
                                                   position);
    events.clear();
    reader.read(events, 10, 1, TimeUnit.SECONDS);

    Assert.assertEquals(6, events.size());
    for (int i = 0; i < 6; i++) {
      StreamEvent event = events.get(i);
      Assert.assertEquals((long) (i + 4), event.getTimestamp());
      Assert.assertEquals("Testing " + event.getTimestamp(), Charsets.UTF_8.decode(event.getBody()).toString());
    }
  }

  @Test
  public void testOffset() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Writer 100 events with different timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           10L);

    for (int i = 0; i < 100; i++) {
      writer.append(StreamFileTestUtils.createEvent(i, "Testing " + i));
    }
    writer.close();

    StreamDataFileIndex index = new StreamDataFileIndex(Locations.newInputSupplier(indexFile));
    StreamDataFileIndexIterator iterator = index.indexIterator();
    while (iterator.nextIndexEntry()) {
      StreamDataFileReader reader = StreamDataFileReader.createWithOffset(
        Locations.newInputSupplier(eventFile),
        Locations.newInputSupplier(indexFile),
        iterator.currentPosition() - 1);
      List<StreamEvent> events = Lists.newArrayList();
      Assert.assertEquals(1, reader.read(events, 1, 0, TimeUnit.SECONDS));
      Assert.assertEquals(iterator.currentTimestamp(), events.get(0).getTimestamp());
    }
  }

  @Test
  public void testEndOfFile() throws Exception {
    // This test is for opening a reader with start time beyond the last event in the file.

    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Write 5 events
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           10000L);
    for (int i = 0; i < 5; i++) {
      writer.append(StreamFileTestUtils.createEvent(i, "Testing " + i));
    }
    writer.close();

    // Open a reader with timestamp larger that all events in the file.
    StreamDataFileReader reader = StreamDataFileReader.createByStartTime(
      Locations.newInputSupplier(eventFile),
      Locations.newInputSupplier(indexFile),
      10L);
    List<StreamEvent> events = Lists.newArrayList();
    Assert.assertEquals(-1, reader.read(events, 10, 1, TimeUnit.SECONDS));

    reader.close();
  }

  @Test
  public void testIndexIterator() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Write 1000 events with different timestamps, and create index for every 100 timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           100L);
    for (int i = 0; i < 1000; i++) {
      writer.append(StreamFileTestUtils.createEvent(1000 + i, "Testing " + i));
    }
    writer.close();

    // Iterate the index
    StreamDataFileIndex index = new StreamDataFileIndex(Locations.newInputSupplier(indexFile));
    StreamDataFileIndexIterator iterator = index.indexIterator();

    long ts = 1000;
    while (iterator.nextIndexEntry()) {
      Assert.assertEquals(ts, iterator.currentTimestamp());
      StreamDataFileReader reader = StreamDataFileReader.createWithOffset(
        Locations.newInputSupplier(eventFile),
        Locations.newInputSupplier(indexFile),
        iterator.currentPosition());
      List<StreamEvent> events = Lists.newArrayList();
      Assert.assertEquals(1, reader.read(events, 1, 0, TimeUnit.SECONDS));
      Assert.assertEquals("Testing " + (ts - 1000),
                          Charsets.UTF_8.decode(events.get(0).getBody()).toString());

      ts += 100;
    }

    Assert.assertEquals(2000, ts);
  }

  @Test
  public void testMaxEvents() throws Exception {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Write 1000 events with 100 different timestamps, and create index for every 100ms timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           100L);

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 10; j++) {
        writer.append(StreamFileTestUtils.createEvent(i, "Testing " + (i * 10 + j)));
      }
    }
    writer.close();

    // Reads events one by one
    List<StreamEvent> events = Lists.newArrayList();
    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));

    int expectedId = 0;
    while (reader.read(events, 1, 1, TimeUnit.SECONDS) >= 0) {
      Assert.assertEquals(1, events.size());
      StreamEvent event = events.get(0);

      long expectedTimestamp = expectedId / 10;

      Assert.assertEquals(expectedTimestamp, event.getTimestamp());
      Assert.assertEquals("Testing " + expectedId, Charsets.UTF_8.decode(event.getBody()).toString());

      expectedId++;
      events.clear();
    }

    reader.close();

    // Reads four events every time, with a new reader.
    events.clear();
    reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));
    int expectedSize = 4;
    while (reader.read(events, 4, 1, TimeUnit.SECONDS) >= 0) {
      Assert.assertEquals(expectedSize, events.size());
      expectedSize += 4;

      long position = reader.getPosition();
      reader.close();
      reader = StreamDataFileReader.createWithOffset(Locations.newInputSupplier(eventFile),
                                                     Locations.newInputSupplier(indexFile),
                                                     position);
    }

    // Verify all events are read
    Assert.assertEquals(1000, events.size());
    expectedId = 0;
    for (StreamEvent event : events) {
      long expectedTimestamp = expectedId / 10;

      Assert.assertEquals(expectedTimestamp, event.getTimestamp());
      Assert.assertEquals("Testing " + expectedId, Charsets.UTF_8.decode(event.getBody()).toString());

      expectedId++;
    }
  }

  @Test
  public void testTailNotExists() throws IOException, InterruptedException {
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Create a read on non-exist file and try reading, it should be ok with 0 events read.
    List<StreamEvent> events = Lists.newArrayList();
    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));
    Assert.assertEquals(0, reader.read(events, 1, 0, TimeUnit.SECONDS));

    // Write an event
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           100L);
    writer.append(StreamFileTestUtils.createEvent(100, "Testing"));
    writer.flush();

    // Reads the event just written
    Assert.assertEquals(1, reader.read(events, 1, 0, TimeUnit.SECONDS));
    Assert.assertEquals(100, events.get(0).getTimestamp());
    Assert.assertEquals("Testing", Charsets.UTF_8.decode(events.get(0).getBody()).toString());

    // Close the writer.
    writer.close();

    // Reader should return EOF (after some time, as closing of file takes time on HDFS.
    Assert.assertEquals(-1, reader.read(events, 1, 2, TimeUnit.SECONDS));
  }

  @Test
  public void testOffsetAtEnd() throws IOException, InterruptedException {
    // Test for offset at the end of file
    Location dir = StreamFileTestUtils.createTempDir(getLocationFactory());
    Location eventFile = dir.getTempFile(".dat");
    Location indexFile = dir.getTempFile(".idx");

    // Write 1 event.
    StreamDataFileWriter writer = new StreamDataFileWriter(Locations.newOutputSupplier(eventFile),
                                                           Locations.newOutputSupplier(indexFile),
                                                           100L);
    writer.append(StreamFileTestUtils.createEvent(1, "Testing"));
    writer.close();

    // Read 1 event.
    List<StreamEvent> events = Lists.newArrayList();
    StreamDataFileReader reader = StreamDataFileReader.create(Locations.newInputSupplier(eventFile));
    Assert.assertEquals(1, reader.read(events, 10, 0, TimeUnit.SECONDS));

    // Create a reader with the offset pointing to EOF timestamp.
    long offset = reader.getPosition();

    reader = StreamDataFileReader.createWithOffset(
      Locations.newInputSupplier(eventFile), Locations.newInputSupplier(indexFile), offset);

    Assert.assertEquals(-1, reader.read(events, 10, 0, TimeUnit.SECONDS));

    // Create a read with offset way pass EOF
    reader = StreamDataFileReader.createWithOffset(
      Locations.newInputSupplier(eventFile),
      Locations.newInputSupplier(indexFile),
      eventFile.length() + 100);

    Assert.assertEquals(-1, reader.read(events, 10, 0, TimeUnit.SECONDS));
  }

  /**
   * Test live stream reader with new partitions and/or sequence file being created over time.
   */
  @Category(SlowTests.class)
  @Test
  public void testLiveStream() throws Exception {
    String streamName = "live";
    final String filePrefix = "prefix";
    StreamAdmin streamAdmin = getStreamAdmin();
    long partitionDuration = 5000;    // 5 seconds

    // Create a stream with 5 seconds partition.
    Properties properties = new Properties();
    properties.setProperty(Constants.Stream.PARTITION_DURATION, Long.toString(partitionDuration));
    streamAdmin.create(streamName, properties);
    final StreamConfig config = streamAdmin.getConfig(streamName);

    // Create a thread that will write 10 event per second
    final AtomicInteger eventsWritten = new AtomicInteger();
    final List<Closeable> closeables = Lists.newArrayList();
    Thread writerThread = new Thread() {
      @Override
      public void run() {
        try {
          while (!interrupted()) {
            FileWriter<StreamEvent> writer = createWriter(config, filePrefix);
            closeables.add(writer);
            for (int i = 0; i < 10; i++) {
              long ts = System.currentTimeMillis();
              writer.append(StreamFileTestUtils.createEvent(ts, "Testing"));
              eventsWritten.getAndIncrement();
            }
            writer.flush();
            TimeUnit.SECONDS.sleep(1);
          }
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
          throw Throwables.propagate(e);
        } catch (InterruptedException e) {
          // No-op
        }
      }
    };

    // Create a live reader start with one partition earlier than current time.
    long partitionStart = StreamUtils.getPartitionStartTime(System.currentTimeMillis() - config.getPartitionDuration(),
                                                            config.getPartitionDuration());
    Location partitionLocation = StreamUtils.createPartitionLocation(config.getLocation(),
                                                                     partitionStart, config.getPartitionDuration());
    Location eventLocation = StreamUtils.createStreamLocation(partitionLocation, filePrefix, 0, StreamFileType.EVENT);

    // Creates a live stream reader that check for sequence file ever 100 millis.
    FileReader<PositionStreamEvent, StreamFileOffset> reader
      = new LiveStreamFileReader(config, new StreamFileOffset(eventLocation, 0L, 0), 100);

    List<StreamEvent> events = Lists.newArrayList();
    // Try to read, since the writer thread is not started, it should get nothing
    Assert.assertEquals(0, reader.read(events, 1, 2, TimeUnit.SECONDS));

    // Start the writer thread.
    writerThread.start();
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    while (stopwatch.elapsedTime(TimeUnit.SECONDS) < 10 && reader.read(events, 1, 1, TimeUnit.SECONDS) == 0) {
      // Empty
    }
    stopwatch.stop();

    // Should be able to read a event
    Assert.assertEquals(1, events.size());

    TimeUnit.MILLISECONDS.sleep(partitionDuration * 2);
    writerThread.interrupt();
    writerThread.join();

    LOG.info("Writer stopped with {} events written.", eventsWritten.get());

    stopwatch.reset();
    while (stopwatch.elapsedTime(TimeUnit.SECONDS) < 10 && events.size() != eventsWritten.get()) {
      reader.read(events, eventsWritten.get(), 0, TimeUnit.SECONDS);
    }

    // Should see all events written
    Assert.assertEquals(eventsWritten.get(), events.size());

    // Take a snapshot of the offset.
    StreamFileOffset offset = new StreamFileOffset(reader.getPosition());

    reader.close();
    for (Closeable c : closeables) {
      Closeables.closeQuietly(c);
    }

    // Now creates a new writer to write 10 more events across two partitions with a skip one partition.
    FileWriter<StreamEvent> writer = createWriter(config, filePrefix);
    try {
      for (int i = 0; i < 5; i++) {
        long ts = System.currentTimeMillis();
        writer.append(StreamFileTestUtils.createEvent(ts, "Testing " + ts));
      }
      TimeUnit.MILLISECONDS.sleep(partitionDuration * 3 / 2);
      for (int i = 0; i < 5; i++) {
        long ts = System.currentTimeMillis();
        writer.append(StreamFileTestUtils.createEvent(ts, "Testing " + ts));
      }
    } finally {
      writer.close();
    }

    // Create a new reader with the previous offset
    reader = new LiveStreamFileReader(config, offset, 100);
    events.clear();
    stopwatch.reset();
    while (stopwatch.elapsedTime(TimeUnit.SECONDS) < 10 && events.size() != 10) {
      reader.read(events, 10, 0, TimeUnit.SECONDS);
    }
    Assert.assertEquals(10, events.size());

    // Try to read more, should got nothing
    reader.read(events, 10, 2, TimeUnit.SECONDS);
    reader.close();

    for (Closeable c : closeables) {
      c.close();
    }
  }


  private FileWriter<StreamEvent> createWriter(StreamConfig config, String prefix) {
    return new TimePartitionedStreamFileWriter(config.getLocation(), config.getPartitionDuration(),
                                               prefix, config.getIndexInterval());
  }
}
