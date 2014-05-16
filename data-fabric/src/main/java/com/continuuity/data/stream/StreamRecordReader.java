/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventDecoder;
import com.continuuity.common.io.Locations;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RecordReader} for reading stream events.
 *
 * @param <K> Key type read by this record reader.
 * @param <V> Value type read by this record reader.
 */
final class StreamRecordReader<K, V> extends RecordReader<K, V> {

  private final StreamEventDecoder<K, V> decoder;
  private final List<StreamEvent> events;

  private StreamDataFileReader reader;
  private StreamInputSplit inputSplit;
  private StreamEventDecoder.DecodeResult<K, V> currentEntry;

  /**
   * Construct a {@link StreamRecordReader} with a given {@link StreamEventDecoder}.
   *
   * @param decoder The decoder to use for decoding stream events.
   */
  StreamRecordReader(StreamEventDecoder<K, V> decoder) {
    this.decoder = decoder;
    this.events = Lists.newArrayListWithCapacity(1);
    this.currentEntry = new StreamEventDecoder.DecodeResult<K, V>();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    inputSplit = (StreamInputSplit) split;
    reader = createReader(FileSystem.get(context.getConfiguration()), inputSplit);
    reader.initialize();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    StreamEvent streamEvent;
    do {
      if (reader.getPosition() >= inputSplit.getStart() + inputSplit.getLength()) {
        return false;
      }

      events.clear();
      if (reader.read(events, 1, 0, TimeUnit.SECONDS) <= 0) {
        return false;
      }
      streamEvent = events.get(0);
    } while (streamEvent.getTimestamp() < inputSplit.getStartTime());

    if (streamEvent.getTimestamp() >= inputSplit.getEndTime()) {
      return false;
    }

    currentEntry = decoder.decode(streamEvent, currentEntry);
    return true;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return currentEntry.getKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return currentEntry.getValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (reader == null) {
      return 0.0f;
    }

    long processed = reader.getPosition() - inputSplit.getStart();
    return Math.min((float) processed / (float) inputSplit.getLength(), 1.0f);
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  /**
   * Creates a {@link StreamDataFileReader} based on the input split.
   *
   * @param fs The {@link FileSystem} for the input.
   * @param inputSplit Split information.
   * @return A {@link StreamRecordReader} that is ready for reading events as specified by the input split.
   */
  private StreamDataFileReader createReader(FileSystem fs, StreamInputSplit inputSplit) {
    return StreamDataFileReader.createWithOffset(Locations.newInputSupplier(fs, inputSplit.getPath()),
                                                 Locations.newInputSupplier(fs, inputSplit.getIndexPath()),
                                                 inputSplit.getStart());
  }
}
