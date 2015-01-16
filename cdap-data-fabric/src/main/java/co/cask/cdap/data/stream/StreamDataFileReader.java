/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.io.BinaryDecoder;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.cdap.common.io.Decoder;
import co.cask.cdap.common.io.SeekableInputStream;
import co.cask.cdap.common.stream.StreamEventDataCodec;
import co.cask.cdap.data.file.FileReader;
import co.cask.cdap.data.file.ReadFilter;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

import java.io.EOFException;
import java.io.FileNotFoundException;
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
public final class StreamDataFileReader implements FileReader<PositionStreamEvent, Long> {

  private final InputSupplier<? extends SeekableInputStream> eventInputSupplier;
  private final InputSupplier<? extends InputStream> indexInputSupplier;
  private final long startTime;
  private final long offset;
  private final byte[] timestampBuffer;
  private final StreamEventBuffer streamEventBuffer;
  private StreamDataFileIndex index;
  private SeekableInputStream eventInput;
  private long position;
  private long timestamp;
  private boolean closed;
  private boolean eof;
  private Decoder decoder;
  private StreamEvent eventTemplate;

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
    this.streamEventBuffer = new StreamEventBuffer();
    this.startTime = startTime;
    this.offset = offset;
    this.timestampBuffer = new byte[8];
    this.timestamp = -1L;
  }

  @Override
  public Long getPosition() {
    return position;
  }

  /**
   * Opens this reader to prepare for consumption. Calling this method is optional as the
   * {@link #read(java.util.Collection, int, long, java.util.concurrent.TimeUnit, co.cask.cdap.data.file.ReadFilter)}
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

          if (stopwatch.elapsedTime(unit) >= timeout) {
            break;
          }

          TimeUnit.NANOSECONDS.sleep(sleepNano);

          if (stopwatch.elapsedTime(unit) >= timeout) {
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
  private StreamDataFileIndex getIndex() {
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

      // If position is <= 0, the reader is not being used yet, hence needs to initialize.
      if (position <= 0) {
        init();
      } else {
        // If position > 0, the reader has already been initialized.
        // We just need to seek to beginning of a data-block, depending on whether there is event in the buffer
        if (streamEventBuffer.hasEvent()) {
          // If there is event in the buffer, we seek to the data block that come after the buffered events
          // to prepare for the reading of the data block after the current buffered events are fully consumed.
          eventInput.seek(streamEventBuffer.getEndPosition());
        } else {
          // Otherwise, we seek to the current position, which should be pointing to the beginning of a data block
          eventInput.seek(position);
        }
      }
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
      initByOffset(offset);
    } else if (startTime > 0) {
      initByTime(startTime);
    }
  }

  private void readHeader() throws IOException {
    // Read the header of the event file
    // First 2 bytes should be 'E' '1'
    byte[] magic = new byte[StreamDataFileConstants.MAGIC_HEADER_SIZE];
    ByteStreams.readFully(eventInput, magic);

    int fileVersion = decodeFileVersion(magic);

    // Read the properties map.
    Map<String, String> properties = StreamUtils.decodeMap(new BinaryDecoder(eventInput));

    verifySchema(properties);

    // Create event template
    if (fileVersion >= 2) {
      eventTemplate = createEventTemplate(properties);
    } else {
      eventTemplate = new StreamEvent(ImmutableMap.<String, String>of(), ByteBuffers.EMPTY_BUFFER, -1L);
    }

    position = eventInput.getPos();
  }

  /**
   * Decodes the file version from the magic header.
   *
   * @return the file version
   * @throws IOException if failed to decode file version from the magic header
   */
  private int decodeFileVersion(byte[] magic) throws IOException {
    if (Arrays.equals(magic, StreamDataFileConstants.MAGIC_HEADER_V1)) {
      return 1;
    }
    if (Arrays.equals(magic, StreamDataFileConstants.MAGIC_HEADER_V2)) {
      return 2;
    }
    throw new IOException(
      String.format("Unsupported stream file format. First two bytes must be %s or %s",
                    Bytes.toStringBinary(StreamDataFileConstants.MAGIC_HEADER_V1),
                    Bytes.toStringBinary(StreamDataFileConstants.MAGIC_HEADER_V2))
    );
  }

  /**
   * Creates a {@link StreamEvent} that will be used as a template for all events consumable from this reader.
   */
  private StreamEvent createEventTemplate(Map<String, String> properties) throws IOException {
    long timestamp = -1L;

    // See if all events in the file are of the same timestamp
    String uniTimestamp = properties.get(StreamDataFileConstants.Property.Key.UNI_TIMESTAMP);
    if (StreamDataFileConstants.Property.Value.CLOSE_TIMESTAMP.equals(uniTimestamp)) {
      // Seek to the end - 8 of the stream to read the close timestamp
      long pos = eventInput.getPos();
      eventInput.seek(eventInput.size() - 8);
      timestamp = Math.abs(readTimestamp());
      eventInput.seek(pos);
    } else if (uniTimestamp != null) {
      timestamp = Long.parseLong(uniTimestamp);
    }

    // Grab the set of default headers for all events
    ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
    String prefix = StreamDataFileConstants.Property.Key.EVENT_HEADER_PREFIX;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        headers.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }

    return new StreamEvent(headers.build(), ByteBuffers.EMPTY_BUFFER, timestamp);
  }

  private void initByOffset(final long offset) throws IOException {
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

  private void initByTime(final long time) throws IOException {
    // If index is provided, lookup the index find the offset closest to start time.
    // If no offset is found, starts from the beginning of the events
    StreamDataFileIndex index = getIndex();
    long offset = index == null ? 0 : index.floorPositionByTime(time);
    if (offset > 0) {
      eventInput.seek(offset);
    }

    skipUntil(new SkipCondition() {
      @Override
      public boolean apply(long position, long timestamp) {
        return timestamp >= time;
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
        // Read timestamp
        long timestamp = readTimestamp();

        // If EOF or condition match, upper bound found. Break the loop.
        eof = timestamp < 0;
        if (eof || condition.apply(positionBound, timestamp)) {
          break;
        }

        int len = readLength();
        position = positionBound;

        // Jump to next timestamp
        eventInput.seek(eventInput.getPos() + len);
        positionBound = eventInput.getPos();

        // need to check this here before we loop around again because it's possible the condition was
        // satisfied by moving up the position even though the timestamp has not changed yet.
        if (condition.apply(positionBound, timestamp)) {
          break;
        }
      }

      if (eof) {
        position = positionBound;
        return;
      }

      // search for the exact StreamData position within the bound.
      eventInput.seek(position);
      readDataBlock(ReadFilter.ALWAYS_ACCEPT);
      while (position < positionBound) {
        if (condition.apply(position, timestamp)) {
          break;
        }
        nextStreamEvent(ReadFilter.ALWAYS_REJECT_OFFSET);
      }
    } catch (IOException e) {
      // It's ok if hitting EOF, meaning it's could be a live stream file or closed by a dead stream handler.
      if (!(e instanceof EOFException)) {
        throw e;
      }
    }
  }

  private void verifySchema(Map<String, String> properties) throws IOException {
    String schemaKey = StreamDataFileConstants.Property.Key.SCHEMA;
    String schemaStr = properties.get(schemaKey);
    if (schemaStr == null) {
      throw new IOException("Missing '" + schemaKey + "' property.");
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
    return decoder.readInt();
  }

  private void readDataBlock(ReadFilter filter) throws IOException {
    // Data block is <timestamp> <length> <stream_data>+
    long timestamp = readTimestamp();
    if (timestamp < 0) {
      eof = true;
      return;
    }

    filter.reset();

    // Use the template timestamp if available
    timestamp = eventTemplate.getTimestamp() >= 0 ? eventTemplate.getTimestamp() : timestamp;
    if (filter.acceptTimestamp(timestamp)) {
      streamEventBuffer.fillBuffer(eventInput, readLength());
      this.timestamp = timestamp;
      return;
    }

    // If timestamp is not accepted and the timestamp comes from event template, then the whole file can be skipped
    if (eventTemplate.getTimestamp() >= 0) {
      eof = true;
      return;
    }

    long nextTimestamp = filter.getNextTimestampHint();
    if (nextTimestamp > timestamp) {
      initByTime(nextTimestamp);
      readDataBlock(filter);
      return;
    }

    int length = readLength();
    long bytesSkipped = eventInput.skip(length);
    if (bytesSkipped != length) {
      throw new EOFException("Expected to skip " + length + " but only " + bytesSkipped + " was skipped.");
    }
  }

  /**
   * Reads or skips a {@link StreamEvent}.
   *
   * @param filter to determine to accept or skip a stream event by offset
   *               and accept or skip a stream event block by timestamp.
   * @return The next StreamEvent or {@code null} if the event is rejected by the filter or reached EOF.
   */
  private PositionStreamEvent nextStreamEvent(ReadFilter filter) throws IOException {
    while (!eof && !streamEventBuffer.hasEvent()) {
      readDataBlock(filter);
    }
    if (eof) {
      return null;
    }

    PositionStreamEvent event = streamEventBuffer.nextEvent(timestamp, eventTemplate.getHeaders(), filter);
    position = streamEventBuffer.getPosition();
    return event;
  }

  private interface SkipCondition {
    boolean apply(long position, long timestamp);
  }
}
