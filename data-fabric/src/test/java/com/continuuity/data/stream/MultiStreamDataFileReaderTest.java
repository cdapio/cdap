/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MultiStreamDataFileReaderTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testMultiFileReader() throws IOException, InterruptedException {
    File dir = tmpFolder.newFolder();

    // Write out 200 events in 5 files, with interleaving timestamps
    List<StreamDataFileWriter> writers = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      File eventFile = new File(dir, String.format("bucket.%d.0.%s", i, StreamFileType.EVENT.getSuffix()));
      File indexFile = new File(dir, String.format("bucket.%d.0.%s", i, StreamFileType.INDEX.getSuffix()));

      StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                             Files.newOutputStreamSupplier(indexFile),
                                                             100L);
      writers.add(writer);
      for (int j = 0; j < 200; j++) {
        long timestamp = j * 5 + i;
        writer.write(timestamp, Iterators.singletonIterator(StreamFileTestUtils.createData("Testing " + timestamp)));
      }
    }

    // Create a multi stream file reader
    List<StreamDataFileSource> sources = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      File eventFile = new File(dir, String.format("bucket.%d.0.%s", i, StreamFileType.EVENT.getSuffix()));

      sources.add(
        new StreamDataFileSource(i, 0,
                                 StreamDataFileReader.create(StreamFileTestUtils.createInputSupplier(eventFile))));
    }

    // Reads all events written so far.
    MultiStreamDataFileReader reader = new MultiStreamDataFileReader(sources);
    List<StreamEvent> events = Lists.newArrayList();
    long expectedTimestamp = 0L;
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(100, reader.next(events, 100, 0, TimeUnit.SECONDS));
      Assert.assertEquals(100, events.size());

      for (StreamEvent event : events) {
        Assert.assertEquals(expectedTimestamp, event.getTimestamp());
        Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
        expectedTimestamp++;
      }
      events.clear();
    }

    Assert.assertEquals(0, reader.next(events, 1, 1, TimeUnit.SECONDS));

    // Writes some more events to the first three writers.
    for (int i = 0; i < 3; i++) {
      StreamDataFileWriter writer = writers.get(i);
      for (int j = 0; j < 10; j++) {
        long timestamp = 1000 + j * 3 + i;
        writer.write(timestamp, Iterators.singletonIterator(StreamFileTestUtils.createData("Testing " + timestamp)));
      }
    }

    // Close all writers
    for (StreamDataFileWriter writer : writers) {
      writer.close();
    }

    // Continue to read
    Assert.assertEquals(30, reader.next(events, 30, 0, TimeUnit.SECONDS));
    Assert.assertEquals(30, events.size());
    for (StreamEvent event : events) {
      Assert.assertEquals(expectedTimestamp, event.getTimestamp());
      Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
      expectedTimestamp++;
    }

    Assert.assertEquals(-1, reader.next(events, 1, 0, TimeUnit.SECONDS));
    reader.close();
  }

  @Test
  public void testOffsets() throws IOException, InterruptedException {
    File dir = tmpFolder.newFolder();

    // Write out 200 events in 5 files, with interleaving timestamps
    for (int i = 0; i < 5; i++) {
      File eventFile = new File(dir, String.format("bucket.%d.0.%s", i, StreamFileType.EVENT.getSuffix()));
      File indexFile = new File(dir, String.format("bucket.%d.0.%s", i, StreamFileType.INDEX.getSuffix()));

      StreamDataFileWriter writer = new StreamDataFileWriter(Files.newOutputStreamSupplier(eventFile),
                                                             Files.newOutputStreamSupplier(indexFile),
                                                             100L);
      for (int j = 0; j < 200; j++) {
        long timestamp = j * 5 + i;
        writer.write(timestamp, Iterators.singletonIterator(StreamFileTestUtils.createData("Testing " + timestamp)));
      }
      writer.close();
    }

    // Create a multi reader
    List<StreamDataFileSource> sources = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      File eventFile = new File(dir, String.format("bucket.%d.0.%s", i, StreamFileType.EVENT.getSuffix()));

      sources.add(
        new StreamDataFileSource(i, 0,
                                 StreamDataFileReader.create(StreamFileTestUtils.createInputSupplier(eventFile))));
    }

    // Reads some events
    MultiStreamDataFileReader reader = new MultiStreamDataFileReader(sources);
    List<StreamEvent> events = Lists.newArrayList();
    long expectedTimestamp = 0L;

    // Read 250 events, in batch size of 10.
    for (int i = 0; i < 25; i++) {
      Assert.assertEquals(10, reader.next(events, 10, 0, TimeUnit.SECONDS));
      Assert.assertEquals(10, events.size());
      for (StreamEvent event : events) {
        Assert.assertEquals(expectedTimestamp, event.getTimestamp());
        Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
        expectedTimestamp++;
      }
      events.clear();
    }
    Iterable<StreamOffset> offsets = reader.getOffset();
    reader.close();

    // Create another multi reader with the offsets
    sources.clear();
    for (StreamOffset offset : offsets) {
      File eventFile = new File(dir, String.format("bucket.%d.%d.%s",
                                                   offset.getBucketId(),
                                                   offset.getBucketSequence(),
                                                   StreamFileType.EVENT.getSuffix()));
      File indexFile = new File(dir, String.format("bucket.%d.%d.%s",
                                                   offset.getBucketId(),
                                                   offset.getBucketSequence(),
                                                   StreamFileType.INDEX.getSuffix()));

      StreamDataFileReader fileReader = StreamDataFileReader.createWithOffset(
        StreamFileTestUtils.createInputSupplier(eventFile),
        StreamFileTestUtils.createInputSupplier(indexFile),
        offset.getOffset());

      sources.add(new StreamDataFileSource(offset.getBucketId(), offset.getBucketSequence(), fileReader));
    }

    // Read 750 events, in batch size of 10.
    reader = new MultiStreamDataFileReader(sources);
    for (int i = 0; i < 75; i++) {
      Assert.assertEquals(10, reader.next(events, 10, 0, TimeUnit.SECONDS));
      Assert.assertEquals(10, events.size());
      for (StreamEvent event : events) {
        Assert.assertEquals(expectedTimestamp, event.getTimestamp());
        Assert.assertEquals("Testing " + expectedTimestamp, Charsets.UTF_8.decode(event.getBody()).toString());
        expectedTimestamp++;
      }
      events.clear();
    }

    Assert.assertEquals(-1, reader.next(events, 10, 0, TimeUnit.SECONDS));

    reader.close();
  }
}
