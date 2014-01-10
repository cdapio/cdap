/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.io.SeekableInputStream;
import com.continuuity.common.stream.DefaultStreamEventData;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test cases for StreamDataFileReader/Writer.
 */
public class StreamDataFileTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Test for basic read write to verify data encode/decode correctly.
   * @throws Exception
   */
  @Test
  public void testBasicReadWrite() throws Exception {
    File eventFile = tmpFolder.newFile();
    File indexFile = tmpFolder.newFile();

    StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                           Files.newOutputStreamSupplier(indexFile),
                                                           10000L);

    // Write 100 events to the stream, with 20 even timestamps
    for (int i = 0; i < 40; i += 2) {
      final int timestamp = i;
      writer.write(timestamp, new AbstractIterator<StreamEventData>() {
        int count = 0;
        @Override
        protected StreamEventData computeNext() {
          if (count++ < 5) {
            return createData("Basic test " + timestamp);
          }
          return endOfData();
        }
      });
    }

    writer.close();

    // Create a reader that starts from beginning.
    StreamDataFileReader reader = StreamDataFileReader.create(createInputSupplier(eventFile));
    List<StreamEvent> events = Lists.newArrayList();
    Assert.assertEquals(100, reader.next(events, 100, 1, TimeUnit.SECONDS));
    Assert.assertEquals(-1, reader.next(events, 100, 1, TimeUnit.SECONDS));

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
    final File eventFile = tmpFolder.newFile();
    final File indexFile = tmpFolder.newFile();

    final CountDownLatch writerStarted = new CountDownLatch(1);
    // Create a thread for writing 10 events, 1 event per 200 milliseconds.
    Thread writerThread = new Thread() {
      @Override
      public void run() {
        try {
          StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                                 Files.newOutputStreamSupplier(indexFile),
                                                                 10000L);
          writerStarted.countDown();

          for (int i = 0; i < 10; i++) {
            writer.write(i, Iterators.singletonIterator(createData("Testing " + i)));
            TimeUnit.MILLISECONDS.sleep(200);
          }
          writer.close();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };

    StreamDataFileReader reader = StreamDataFileReader.create(createInputSupplier(eventFile));
    List<StreamEvent> events = Lists.newArrayList();

    writerThread.start();
    writerStarted.await();

    // Expect 10 events, followed by EOF.
    Assert.assertEquals(5, reader.next(events, 5, 1200, TimeUnit.MILLISECONDS));
    Assert.assertEquals(5, reader.next(events, 5, 1200, TimeUnit.MILLISECONDS));
    Assert.assertEquals(-1, reader.next(events, 1, 500, TimeUnit.MILLISECONDS));

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
    File eventFile = tmpFolder.newFile();
    File indexFile = tmpFolder.newFile();

    // Write 1000 events with different timestamps, and create index for every 100 timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                           Files.newOutputStreamSupplier(indexFile),
                                                           100L);
    for (int i = 0; i < 1000; i++) {
      writer.write(1000 + i, Iterators.singletonIterator(createData("Testing " + i)));
    }
    writer.close();

    // Read with index
    for (long ts : new long[] {1050, 1110, 1200, 1290, 1301, 1400, 1500, 1600, 1898, 1900, 1999}) {
      StreamDataFileReader reader = StreamDataFileReader.createByStartTime(createInputSupplier(eventFile),
                                                                           createInputSupplier(indexFile),
                                                                           ts);
      Queue<StreamEvent> events = Lists.newLinkedList();
      Assert.assertEquals(1, reader.next(events, 1, 1L, TimeUnit.MILLISECONDS));
      Assert.assertEquals(ts, events.poll().getTimestamp());

      reader.close();
    }
  }

  @Test
  public void testPosition() throws Exception {
    File eventFile = tmpFolder.newFile();
    File indexFile = tmpFolder.newFile();

    // Writer 10 events with different timestamps. Index doesn't matter
    StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                           Files.newOutputStreamSupplier(indexFile),
                                                           100L);

    for (int i = 0; i < 10; i++) {
      writer.write(i, Iterators.singletonIterator(createData("Testing " + i)));
    }
    writer.close();

    // Read 4 events
    StreamDataFileReader reader = StreamDataFileReader.create(createInputSupplier(eventFile));
    List<StreamEvent> events = Lists.newArrayList();
    reader.next(events, 4, 1, TimeUnit.SECONDS);

    Assert.assertEquals(4, events.size());

    for (StreamEvent event : events) {
      Assert.assertEquals("Testing " + event.getTimestamp(), Charsets.UTF_8.decode(event.getBody()).toString());
    }

    long position = reader.position();
    reader.close();

    // Open a new reader, read from the last position.
    reader = StreamDataFileReader.createByPosition(createInputSupplier(eventFile), position);
    events.clear();
    reader.next(events, 10, 1, TimeUnit.SECONDS);

    Assert.assertEquals(6, events.size());
    for (int i = 0; i < 6; i++) {
      StreamEvent event = events.get(i);
      Assert.assertEquals((long) (i + 4), event.getTimestamp());
      Assert.assertEquals("Testing " + event.getTimestamp(), Charsets.UTF_8.decode(event.getBody()).toString());
    }
  }

  @Test
  public void testOffset() throws Exception {
    File eventFile = tmpFolder.newFile();
    File indexFile = tmpFolder.newFile();

    // Writer 100 events with different timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                           Files.newOutputStreamSupplier(indexFile),
                                                           10L);

    for (int i = 0; i < 100; i++) {
      writer.write(i, Iterators.singletonIterator(createData("Testing " + i)));
    }
    writer.close();

    StreamDataFileIndex index = new StreamDataFileIndex(createInputSupplier(indexFile));
    StreamDataFileIndexIterator iterator = index.indexIterator();
    while (iterator.nextIndexEntry()) {
      StreamDataFileReader reader = StreamDataFileReader.createWithOffset(createInputSupplier(eventFile),
                                                                          createInputSupplier(indexFile),
                                                                          iterator.currentPosition() - 1);
      List<StreamEvent> events = Lists.newArrayList();
      Assert.assertEquals(1, reader.next(events, 1, 0, TimeUnit.SECONDS));
      Assert.assertEquals(iterator.currentTimestamp(), events.get(0).getTimestamp());
    }
  }

  @Test
  public void testEndOfFile() throws Exception {
    // This test is for opening a reader with start time beyond the last event in the file.

    File eventFile = tmpFolder.newFile();
    File indexFile = tmpFolder.newFile();

    // Write 5 events
    StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                           Files.newOutputStreamSupplier(indexFile),
                                                           10000L);
    for (int i = 0; i < 5; i++) {
      writer.write(i, Iterators.singletonIterator(createData("Testing " + i)));
    }
    writer.close();

    // Open a reader with timestamp larger that all events in the file.
    StreamDataFileReader reader = StreamDataFileReader.createByStartTime(createInputSupplier(eventFile),
                                                                         createInputSupplier(indexFile),
                                                                         10L);
    List<StreamEvent> events = Lists.newArrayList();
    Assert.assertEquals(-1, reader.next(events, 10, 1, TimeUnit.SECONDS));

    reader.close();
  }

  @Test
  public void testIndexIterator() throws Exception {
    File eventFile = tmpFolder.newFile();
    File indexFile = tmpFolder.newFile();

    // Write 1000 events with different timestamps, and create index for every 100 timestamps.
    StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                           Files.newOutputStreamSupplier(indexFile),
                                                           100L);
    for (int i = 0; i < 1000; i++) {
      writer.write(1000 + i, Iterators.singletonIterator(createData("Testing " + i)));
    }
    writer.close();

    // Iterate the index
    StreamDataFileIndex index = new StreamDataFileIndex(createInputSupplier(indexFile));
    StreamDataFileIndexIterator iterator = index.indexIterator();

    long ts = 1000;
    while (iterator.nextIndexEntry()) {
      Assert.assertEquals(ts, iterator.currentTimestamp());
      StreamDataFileReader reader = StreamDataFileReader.createByPosition(createInputSupplier(eventFile),
                                                                          iterator.currentPosition());
      List<StreamEvent> events = Lists.newArrayList();
      Assert.assertEquals(1, reader.next(events, 1, 0, TimeUnit.SECONDS));
      Assert.assertEquals("Testing " + (ts - 1000),
                          Charsets.UTF_8.decode(events.get(0).getBody()).toString());

      ts += 100;
    }

    Assert.assertEquals(2000, ts);
  }

  private InputSupplier<? extends SeekableInputStream> createInputSupplier(final File file) {
    return new InputSupplier<SeekableInputStream>() {
      @Override
      public SeekableInputStream getInput() throws IOException {
        return SeekableInputStream.create(new FileInputStream(file));
      }
    };
  }

  private StreamEventData createData(String body) {
    return new DefaultStreamEventData(ImmutableMap.<String, String>of(), Charsets.UTF_8.encode(body));
  }
}
