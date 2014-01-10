package com.continuuity.data.stream;

import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.BufferedEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.common.stream.StreamEventDataCodec;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.fs.Syncable;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * File format
 *
 * Stream event file:
 *
 * <pre>
 * {@code
 *
 * event_file = <header> <data>* <end_marker>
 * header = "E" "1" <properties>
 * properties = Avro encoded with the properties schema
 * data = <timestamp> <length> <count> <stream_event>+
 * timestamp = 8 bytes int64 for timestamp in milliseconds
 * length = Avro encoded int64 for size in bytes for <count> + all <stream_event>s
 * count = Avro encoded int32 for number of <stream_event>s
 * stream_event = Avro encoded bytes according to the StreamData schema
 * end_marker = 8 bytes int64 with value == -1
 *
 * }
 * </pre>
 *
 * Stream index file:
 *
 * <pre>
 * {@code
 *
 * meta_file = <header> <index>*
 * header = "I" "1" <properties>
 * properties = Avro encoded with the properties schema
 * index = <timestamp> <offset>
 * timestamp = 8 bytes int64 for timestamp in milliseconds
 * offset = 8 bytes int64 for offset to data block in the event file
 *
 * }
 * </pre>
 */
@NotThreadSafe
public final class StreamDataFileWriter implements Closeable, Flushable {

  private static final byte[] MAGIC_HEADER = {'E', '1'};
  private static final byte[] INDEX_MAGIC_HEADER = {'I', '1'};

  private final OutputStream eventOutput;
  private final OutputStream indexOutput;
  private final long indexInterval;
  private final ByteBuffer buffer;
  private final BufferedEncoder sizeEncoder;
  private final BufferedEncoder countEncoder;
  private final BufferedEncoder eventEncoder;
  private long position;
  private long nextIndexTime;
  private boolean closed;

  /**
   * Constructs a new instance that writes to given outputs.
   *
   * @param eventOutputSupplier Provides {@link OutputStream} for writing events.
   * @param indexOutputSupplier Provides {@link OutputStream} for writing index.
   * @param indexInterval Time interval in milliseconds for emitting new index entry.
   * @throws IOException If there is error in preparing the output streams.
   */
  public StreamDataFileWriter(OutputSupplier<? extends OutputStream> eventOutputSupplier,
                              OutputSupplier<? extends OutputStream> indexOutputSupplier,
                              long indexInterval) throws IOException {

    this.eventOutput = eventOutputSupplier.getOutput();
    try {
      this.indexOutput = indexOutputSupplier.getOutput();
    } catch (IOException e) {
      Closeables.closeQuietly(this.eventOutput);
      throw e;
    }
    this.indexInterval = indexInterval;

    this.buffer = ByteBuffer.allocate(Longs.BYTES * 2);
    this.sizeEncoder = new BufferedEncoder(Longs.BYTES + 1, createEncoderFactory());
    this.countEncoder = new BufferedEncoder(Ints.BYTES + 1, createEncoderFactory());
    this.eventEncoder = new BufferedEncoder(32768, createEncoderFactory());

    try {
      init();
    } catch (IOException e) {
      Closeables.closeQuietly(eventOutput);
      Closeables.closeQuietly(indexOutput);
      throw e;
    }
  }

  /**
   * Writes a series of {@link com.continuuity.api.stream.StreamEventData} with the given timestamp.
   *
   * @param timestamp Timestamp of the event.
   * @param events Iterator to provides the {@link com.continuuity.api.stream.StreamEventData} to be written.
   * @throws IOException If there is error during writing.
   */
  public void write(long timestamp, Iterator<StreamEventData> events) throws IOException {
    if (closed) {
      throw new IOException("Writer already closed.");
    }

    // Record the current event output position if needs to update index
    long indexOffset = -1L;
    if (timestamp >= nextIndexTime) {
      indexOffset = position;
    }

    try {
      // Write to event output first.
      // Each data block is <timestamp> <length> <count> <stream_event>+

      // Encode all events to get count.
      eventEncoder.reset();
      int count = 0;
      while (events.hasNext()) {
        StreamEventDataCodec.encode(events.next(), eventEncoder);
        count++;
      }

      // Encode count
      countEncoder.reset();
      countEncoder.writeInt(count);

      // Encode size = size of count + size of events
      long size = countEncoder.size() + eventEncoder.size();
      sizeEncoder.reset();
      sizeEncoder.writeLong(size);

      // Write the 8 bytes <timestamp>, followed by size, count, and events
      buffer.clear();
      buffer.putLong(timestamp);

      eventOutput.write(buffer.array(), 0, buffer.position());
      sizeEncoder.writeTo(eventOutput);
      countEncoder.writeTo(eventOutput);
      eventEncoder.writeTo(eventOutput);

      // Sync to event output
      sync(eventOutput);
      // Position moves 8 bytes (timestamp) + size of "size" + size
      position += Longs.BYTES + sizeEncoder.size() + size;

      // If needs to write a new index, write it and update the nextIndexTime.
      if (indexOffset >= 0) {
        buffer.clear();
        buffer.putLong(timestamp)
              .putLong(indexOffset);
        indexOutput.write(buffer.array(), 0, buffer.position());
        sync(indexOutput);
        nextIndexTime = timestamp + indexInterval;
      }
    } catch (IOException e) {
      // If there is any IOException, close this writer to avoid further writes.
      closed = true;
      Closeables.closeQuietly(eventOutput);
      Closeables.closeQuietly(indexOutput);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      // Write the tail marker, which is a -1 timestamp.
      eventOutput.write(Longs.toByteArray(-1L));
    } finally {
      closed = true;
      try {
        eventOutput.close();
      } finally {
        indexOutput.close();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    sync(eventOutput);
    sync(indexOutput);
  }

  private void init() throws IOException {
    // Writes the header for event file
    eventOutput.write(MAGIC_HEADER);

    long headerSize = 2;

    BufferedEncoder encoder = new BufferedEncoder(1024, createEncoderFactory());
    StreamUtils.encodeMap(ImmutableMap.of("stream.schema",
                                          StreamEventDataCodec.STREAM_DATA_SCHEMA.toString()), encoder);
    headerSize += encoder.size();
    encoder.writeTo(eventOutput);
    sync(eventOutput);
    position = headerSize;

    // Writes the header for index file
    indexOutput.write(INDEX_MAGIC_HEADER);

    encoder.reset();
    // Empty properties map for now. May have properties in future version.
    StreamUtils.encodeMap(ImmutableMap.<String, String>of(), encoder);
    encoder.writeTo(indexOutput);
    sync(indexOutput);
  }

  private void sync(OutputStream output) throws IOException {
    if (output instanceof Syncable) {
      ((Syncable) output).hsync();
    } else {
      output.flush();
    }
  }

  private static Function<OutputStream, Encoder> createEncoderFactory() {
    return new Function<OutputStream, Encoder>() {
      @Override
      public Encoder apply(OutputStream input) {
        return new BinaryEncoder(input);
      }
    };
  }
}
