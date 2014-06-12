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
import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaTypeAdapter;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
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
public final class StreamDataFileReader implements FileReader<PositionStreamEvent, Long> {

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
  private int length;
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
    return new StreamDataFileReader(eventInputSupplier, null, 0L, 0L);
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
    return new StreamDataFileReader(eventInputSupplier, indexInputSupplier, startTime, 0L);
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
    return new StreamDataFileReader(eventInputSupplier, indexInputSupplier, 0L, offset);
  }

  private StreamDataFileReader(InputSupplier<? extends SeekableInputStream> eventInputSupplier,
                               InputSupplier<? extends InputStream> indexInputSupplier,
                               long startTime, long offset) {
    this.eventInputSupplier = eventInputSupplier;
    this.indexInputSupplier = indexInputSupplier;
    this.startTime = startTime;
    this.offset = offset;
    this.timestampBuffer = new byte[8];
    this.timestamp = -1L;
    this.length = -1;
  }

  @Override
  public Long getPosition() {
    return position;
  }

  /**
   * Opens this reader to prepare for consumption. Calling this method is optional as the
   * {@link #read(java.util.Collection, int, long, java.util.concurrent.TimeUnit, com.continuuity.data.file.ReadFilter)}
   * method would do the initialization if this method hasn't been called.
   *
   * @throws IOException If there is error initializing.
   */
  @Override
  public void initialize() throws IOException {
    try {
      if (eventInput == null) {
        doOpen();
      }
    } catch (IOException e) {
      if (!(e instanceof EOFException || e instanceof FileNotFoundException)) {
        throw e;
      }
      // It's ok if the file doesn't exists or EOF. As that's the tailing behavior.
    }
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

  @Override
  public int read(Collection<? super PositionStreamEvent> events, int maxEvents,
                  long timeout, TimeUnit unit) throws IOException, InterruptedException {
    return read(events, maxEvents, timeout, unit, ReadFilter.ALWAYS_ACCEPT);
  }

  @Override
  public int read(Collection<? super PositionStreamEvent> events, int maxEvents,
                  long timeout, TimeUnit unit, ReadFilter readFilter) throws IOException, InterruptedException {
    if (closed) {
      throw new IOException("Reader already closed.");
    }

    int eventCount = 0;
    long sleepNano = computeSleepNano(timeout, unit);
    try {
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();

      // Keep reading events until max events.
      while (!eof && eventCount < maxEvents) {
        try {
          if (eventInput == null) {
            doOpen();
          }

          PositionStreamEvent event = nextStreamEvent(readFilter);
          if (event != null) {
            events.add(event);
            eventCount++;
          } else if (eof) {
            break;
          }

          position = eventInput.getPos();

        } catch (IOException e) {
          if (eventInput != null) {
            eventInput.close();
            eventInput = null;
          }

          if (!(e instanceof EOFException || e instanceof FileNotFoundException)) {
            throw e;
          }

          // If end of stream file or no timeout is allowed, break the loop.
          if (eof || timeout <= 0) {
            break;
          }

          if (stopwatch.elapsedTime(unit) > timeout) {
            break;
          }

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
      index = new StreamDataFileIndex(indexInputSupplier);
    }
    return index;
  }

  /**
   * Opens and initialize this reader.
   */
  private void doOpen() throws IOException {
    try {
      eventInput = eventInputSupplier.getInput();
      decoder = new BinaryDecoder(eventInput);

      if (position <= 0) {
        init();
      }
      eventInput.seek(position);
    } catch (IOException e) {
      position = 0;
      if (eventInput != null) {
        eventInput.close();
        eventInput = null;
      }
      throw e;
    }
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
    long positionBound = position = eventInput.getPos();

    try {
      while (!eof) {
        positionBound = eventInput.getPos();

        // Read timestamp
        long timestamp = readTimestamp();

        // If EOF or condition match, upper bound found. Break the loop.
        if (timestamp == -1L || condition.apply(positionBound, timestamp)) {
          break;
        }

        int len = readLength();
        position = positionBound;

        // Jump to next timestamp
        eventInput.seek(eventInput.getPos() + len);
      }

      if (eof) {
        position = positionBound;
        return;
      }

      // search for the exact StreamData position within the bound.
      eventInput.seek(position);
      while (position < positionBound) {
        if (timestamp < 0) {
          timestamp = readTimestamp();
        }
        if (condition.apply(position, timestamp)) {
          break;
        }
        nextStreamEvent(ReadFilter.ALWAYS_REJECT_OFFSET);
        position = eventInput.getPos();
      }
    } catch (IOException e) {
      // It's ok if hitting EOF, meaning it's could be a live stream file or closed by a dead stream handler.
      if (!(e instanceof EOFException)) {
        throw e;
      }
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
    return Bytes.toLong(timestampBuffer);
  }

  private int readLength() throws IOException {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      // If failed to read data block length, reset the timestamp as well,
      // since the position hasn't been updated yet, and is still pointing to timestamp position.
      timestamp = -1L;
      throw e;
    }
  }

  private StreamEventData readStreamData() throws IOException {
    return StreamEventDataCodec.decode(decoder);
  }

  private void skipStreamData() throws IOException {
    StreamEventDataCodec.skip(decoder);
  }

  /**
   * Reads or skips a {@link StreamEvent}.
   *
   * @param filter to determine to accept or skip a stream event by offset
   *               and accept or skip a stream event block by timestamp.
   * @return The next StreamEvent or {@code null} if the event is rejected by the filter or reached EOF.
   */
  private PositionStreamEvent nextStreamEvent(ReadFilter filter) throws IOException {
    // Data block is <timestamp> <length> <stream_data>+
    PositionStreamEvent event = null;
    boolean done = false;

    while (!done) {
      if (timestamp < 0) {
        timestamp = readTimestamp();
      }

      // Timestamp == -1 indicate that's the end of file.
      if (timestamp == -1L) {
        eof = true;
        break;
      }

      boolean isReadBlockLength = length < 0;
      if (isReadBlockLength) {
        length = readLength();
      }

      if (isReadBlockLength && !filter.acceptTimestamp(timestamp)) {
        long bytesSkipped = eventInput.skip(length);
        boolean skippedExpected = (bytesSkipped == length);
        timestamp = -1L;
        length = -1;

        if (skippedExpected) {
          continue;
        } else {
          throw new EOFException();
        }
      }

      if (length > 0) {
        long startPos = eventInput.getPos();

        try {
          if (filter.acceptOffset(startPos)) {
            event = new DefaultPositionStreamEvent(readStreamData(), timestamp, startPos);
          } else {
            skipStreamData();
          }
        } catch (IOException e) {
          // If failed to read first event in the data block, reset the timestamp and length to -1
          // This is because position hasn't been updated yet and retry will start from the timestamp position.
          if (isReadBlockLength) {
            timestamp = -1L;
            length = -1;
          }
          throw e;
        }
        long endPos = eventInput.getPos();
        done = true;
        length -= (int) (endPos - startPos);
      }
      if (length == 0) {
        timestamp = -1L;
        length = -1;
      }
    }

    return event;
  }


  private static final class DefaultPositionStreamEvent implements PositionStreamEvent {

    private final StreamEventData delegate;
    private final long timestamp;
    private final long position;

    private DefaultPositionStreamEvent(StreamEventData delegate, long timestamp, long position) {
      this.delegate = delegate;
      this.timestamp = timestamp;
      this.position = position;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public ByteBuffer getBody() {
      return delegate.getBody();
    }

    @Override
    public Map<String, String> getHeaders() {
      return delegate.getHeaders();
    }

    @Override
    public long getStart() {
      return position;
    }
  }

  private interface SkipCondition {
    boolean apply(long position, long timestamp);
  }
}
