/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.SeekableInputStream;
import com.continuuity.common.stream.StreamEventDataCodec;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for reading data file written by {@link StreamDataFileWriter}.
 *
 * @see StreamDataFileWriter
 */
@NotThreadSafe
public final class StreamDataFileReader implements Closeable {

  private static final byte[] MAGIC_HEADER = {'E', '1'};

  private final InputSupplier<? extends SeekableInputStream> eventInputSupplier;
  private final InputSupplier<? extends InputStream> indexInputSupplier;
  private final long startTime;
  private final long offset;
  private StreamDataFileIndex index;
  private SeekableInputStream eventInput;
  private long position;
  private byte[] timestampBuffer;
  private long timestamp;
  private long length;
  private int count;
  private boolean closed;
  private boolean eof;
  private Decoder decoder;

  /**
   * Opens a new {@link StreamDataFileReader} with the given inputs.
   *
   * @param eventInputSupplier An {@link InputSupplier} for providing the stream to read events.
   * @return A new instance of {@link StreamDataFileReader}.
   */
  public static StreamDataFileReader create(InputSupplier<? extends SeekableInputStream> eventInputSupplier) {
    return new StreamDataFileReader(eventInputSupplier, null, 0L, 0L, 0L);
  }

  /**
   * Opens a new {@link StreamDataFileReader} with the given inputs that starts reading events that are
   * written at or after the given timestamp.
   *
   * @param eventInputSupplier An {@link InputSupplier} for providing the stream to read events.
   * @param indexInputSupplier An {@link InputSupplier} for providing the stream to read event index.
   * @param startTime Timestamp in milliseconds for the event time to start reading with.
   * @return A new instance of {@link StreamDataFileReader}.
   */
  public static StreamDataFileReader createByStartTime(InputSupplier<? extends SeekableInputStream> eventInputSupplier,
                                                       InputSupplier<? extends InputStream> indexInputSupplier,
                                                       long startTime) {
    return new StreamDataFileReader(eventInputSupplier, indexInputSupplier, startTime, 0L, 0L);
  }

  /**
   * Opens a new {@link StreamDataFileReader} with the given inputs and event file position.
   *
   * @param eventInputSupplier An {@link InputSupplier} for providing the stream to read events.
   * @param position Position in the event file to start reading with.
   * @return A new instance of {@link StreamDataFileReader}.
   */
  public static StreamDataFileReader createByPosition(InputSupplier<? extends SeekableInputStream> eventInputSupplier,
                                                      long position) {
    return new StreamDataFileReader(eventInputSupplier, null, 0L, position, 0L);
  }

  /**
   * Opens a new {@link StreamDataFileReader} with the given inputs, which starts reading events at a the smallest
   * event position that is larger than or equal to the given offset.
   *
   * @param eventInputSupplier An {@link InputSupplier} for providing the stream to read events.
   * @param indexInputSupplier An {@link InputSupplier} for providing the stream to read event index.
   * @param offset An arbitrary event file offset.
   * @return A new instance of {@link StreamDataFileReader}.
   */
  public static StreamDataFileReader createWithOffset(InputSupplier<? extends SeekableInputStream> eventInputSupplier,
                                                      InputSupplier<? extends InputStream> indexInputSupplier,
                                                      long offset) {
    return new StreamDataFileReader(eventInputSupplier, indexInputSupplier, 0L, 0L, offset);
  }

  private StreamDataFileReader(InputSupplier <? extends SeekableInputStream> eventInputSupplier,
                              InputSupplier<? extends InputStream> indexInputSupplier,
                              long startTime, long position, long offset) {
    this.eventInputSupplier = eventInputSupplier;
    this.indexInputSupplier = indexInputSupplier;
    this.startTime = startTime;
    this.position = position;
    this.offset = offset;
    this.timestampBuffer = new byte[8];
    this.timestamp = -1L;
    this.length = -1L;
    this.count = -1;
  }

  public long position() {
    return position;
  }

  /**
   * Opens this reader to prepare for consumption.
   *
   * @throws IOException If there is error opening the file.
   */
  public void open() throws IOException {
    eventInput = eventInputSupplier.getInput();

    if (position <= 0) {
      init();
    }
    eventInput.seek(position);
    decoder = new BinaryDecoder(eventInput);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      if (eventInput != null) {
        eventInput.close();
      }
    } finally {
      closed = true;
    }
  }

  /**
   * Reads series of stream events up to the given maximum.
   *
   * @param events Collection to store the result.
   * @param maxEvents Maximum number of events to read.
   * @param timeout Maximum of time to spend on trying to read events
   * @param unit Unit for the timeout.
   *
   * @return Number of events read, potentially {@code 0}.
   *         If no more events due to end of file, {@code -1} is returned.
   * @throws IOException If there is IO error while reading.
   */
  public int next(Collection<StreamEvent> events, int maxEvents,
                  long timeout, TimeUnit unit) throws IOException, InterruptedException {
    if (closed) {
      throw new IOException("Reader already closed.");
    }

    if (eof) {
      return -1;
    }

    int eventCount = 0;
    long sleepNano = computeSleepNano(timeout, unit);
    try {
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();

      // Keep reading events until max events.
      while (eventCount < maxEvents) {
        try {
          if (eventInput == null) {
            open();
          }

          // Data block is <timestamp> <length> <count> <stream_data>+
          if (timestamp < 0) {
            timestamp = readTimestamp();
          }

          // Timestamp == -1 indicate that's the end of file.
          if (timestamp == -1L) {
            eof = true;
            break;
          }

          if (length < 0) {
            length = readLength();
          }
          if (count < 0) {
            count = readCount();
          }

          while (count > 0) {
            events.add(new DefaultStreamEvent(readStreamData(), timestamp));
            count--;
            eventCount++;
          }

          timestamp = -1L;
          length = -1L;
          count = -1;

        } catch (EOFException e) {
          // If it reaches end of file unexpectedly, keep trying until timeout.
          if (timeout <= 0) {
            break;
          }

          if (stopwatch.elapsedTime(unit) > timeout) {
            break;
          }

          eventInput = null;
          TimeUnit.NANOSECONDS.sleep(sleepNano);

          if (stopwatch.elapsedTime(unit) > timeout) {
            break;
          }
        }
      }

      return (eventCount == 0 && eof) ? -1 : eventCount;

    } catch (IOException e) {
      close();
      throw e;
    }
  }


  /**
   * Returns the index for the stream data or {@code null} if index is absent.
   */
  StreamDataFileIndex getIndex() {
    if (index == null && indexInputSupplier != null) {
      return new StreamDataFileIndex(indexInputSupplier);
    }
    return index;
  }

  private long computeSleepNano(long timeout, TimeUnit unit) {
    long sleepNano = TimeUnit.NANOSECONDS.convert(timeout, unit) / 10;
    return sleepNano <= 0 ? 1 : sleepNano;
  }

  private void init() throws IOException {
    readHeader();

    // If it is constructed with an arbitrary offset, need to find an event position
    if (offset > 0) {
      initByOffset();
    } else if (startTime > 0) {
      initByTime();
    }
  }

  private void readHeader() throws IOException {
    // Read the header of the event file
    // First 2 bytes should be 'E' '1'
    byte[] magic = new byte[MAGIC_HEADER.length];
    ByteStreams.readFully(eventInput, magic);

    if (!Arrays.equals(magic, MAGIC_HEADER)) {
      throw new IOException("Unsupported stream file format. Expected magic bytes as 'E' '1'");
    }

    // Read the properties map.
    Map<String, String> properties = StreamUtils.decodeMap(new BinaryDecoder(eventInput));
    verifySchema(properties.get("stream.schema"));

    position = eventInput.getPos();
  }

  private void initByOffset() throws IOException {
    // If index is provided, lookup the position smaller but closest to the offset.
    StreamDataFileIndex index = getIndex();
    long pos = index == null ? 0 : index.floorPosition(offset);
    if (pos > 0) {
      eventInput.seek(pos);
    }

    skipUntil(new SkipCondition() {
      @Override
      public boolean apply(long position, long timestamp) {
        return position >= offset;
      }
    });
  }

  private void initByTime() throws IOException {
    // If index is provided, lookup the index find the offset closest to start time.
    // If no offset is found, starts from the beginning of the events
    StreamDataFileIndex index = getIndex();
    long offset = index == null ? 0 : index.floorPositionByTime(startTime);
    if (offset > 0) {
      eventInput.seek(offset);
    }

    skipUntil(new SkipCondition() {
      @Override
      public boolean apply(long position, long timestamp) {
        return timestamp >= startTime;
      }
    });
  }

  /**
   * Skips events until the given condition is true.
   */
  private void skipUntil(SkipCondition condition) throws IOException {
    Decoder decoder = new BinaryDecoder(eventInput);
    while (!eof) {
      position = eventInput.getPos();

      // Read timestamp
      ByteStreams.readFully(eventInput, timestampBuffer);

      // If timestamp found, break the loop
      long timestamp = Bytes.toLong(timestampBuffer);
      if (timestamp == -1L) {
        eof = true;
        break;
      } else if (condition.apply(position, timestamp)) {
        break;
      }

      // Jump to next timestamp
      long length = decoder.readLong();
      eventInput.seek(eventInput.getPos() + length);
    }
  }

  private void verifySchema(String schemaStr) throws IOException {
    if (schemaStr == null) {
      throw new IOException("Missing 'stream.schema' property.");
    }

    try {
      Schema schema = new SchemaTypeAdapter().read(new JsonReader(new StringReader(schemaStr)));
      if (!StreamEventDataCodec.STREAM_DATA_SCHEMA.equals(schema)) {
        throw new IOException("Unsupported schema " + schemaStr);
      }

    } catch (JsonSyntaxException e) {
      throw new IOException("Invalid schema.", e);
    }
  }

  private long readTimestamp() throws IOException {
    ByteStreams.readFully(eventInput, timestampBuffer);
    position = eventInput.getPos();
    return Bytes.toLong(timestampBuffer);
  }

  private long readLength() throws IOException {
    long len = decoder.readLong();
    position = eventInput.getPos();
    return len;
  }

  private int readCount() throws IOException {
    int count = decoder.readInt();
    position = eventInput.getPos();
    return count;
  }

  private StreamEventData readStreamData() throws IOException {
    StreamEventData data = StreamEventDataCodec.decode(decoder);
    position = eventInput.getPos();
    return data;
  }

  private interface SkipCondition {
    boolean apply(long position, long timestamp);
  }
}
