package com.continuuity.data.stream;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.BufferedEncoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.common.stream.StreamEventDataCodec;
import com.continuuity.data.file.FileWriter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Longs;
import org.apache.hadoop.fs.Syncable;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
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
 * data = <timestamp> <length> <stream_event>+
 * timestamp = 8 bytes int64 for timestamp in milliseconds
 * length = Avro encoded int32 for size in bytes for all <stream_event>s
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
public final class StreamDataFileWriter implements Closeable, Flushable, FileWriter<StreamEvent> {

  private static final byte[] MAGIC_HEADER = {'E', '1'};
  private static final byte[] INDEX_MAGIC_HEADER = {'I', '1'};
  private static final int BUFFER_SIZE = 256 * 1024;    // 256K

  private final OutputStream eventOutput;
  private final OutputStream indexOutput;
  private final long indexInterval;
  private final BufferedEncoder encoder;
  private final BufferedEncoder lengthEncoder;

  // Timestamp for the current block
  private long currentTimestamp;
  private long position;
  private long nextIndexTime;
  private boolean synced;
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
    this.currentTimestamp = -1L;

    Function<OutputStream, Encoder> encoderFactory = createEncoderFactory();
    this.encoder = new BufferedEncoder(BUFFER_SIZE, encoderFactory);
    this.lengthEncoder = new BufferedEncoder(5, encoderFactory);

    try {
      init();
    } catch (IOException e) {
      Closeables.closeQuietly(eventOutput);
      Closeables.closeQuietly(indexOutput);
      throw e;
    }
  }


  @Override
  public void append(StreamEvent event) throws IOException {
    if (closed) {
      throw new IOException("Writer already closed.");
    }

    synced = false;
    long eventTimestamp = event.getTimestamp();
    if (eventTimestamp < currentTimestamp) {
      throw closeWithException(new IOException("Out of order events written."));
    }

    try {
      if (eventTimestamp > currentTimestamp) {
        flushBlock(false);

        currentTimestamp = eventTimestamp;

        // Write the timestamp directly to output
        eventOutput.write(Bytes.toBytes(currentTimestamp));
        position += Bytes.SIZEOF_LONG;
      }

      // Encodes the event data into buffer.
      StreamEventDataCodec.encode(event, encoder);

      // Optionally flush if already filled up the buffer.
      if (encoder.size() >= BUFFER_SIZE) {
        flushBlock(false);
      }

    } catch (IOException e) {
      throw closeWithException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      flushBlock(false);
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
    try {
      flushBlock(true);
    } catch (IOException e) {
      throw closeWithException(e);
    }
  }

  private void init() throws IOException {
    // Writes the header for event file
    encoder.writeRaw(MAGIC_HEADER);

    StreamUtils.encodeMap(ImmutableMap.of("stream.schema",
                                          StreamEventDataCodec.STREAM_DATA_SCHEMA.toString()), encoder);
    long headerSize = encoder.size();
    encoder.writeTo(eventOutput);
    sync(eventOutput);
    position = headerSize;

    // Writes the header for index file
    encoder.writeRaw(INDEX_MAGIC_HEADER);

    // Empty properties map for now. May have properties in future version.
    StreamUtils.encodeMap(ImmutableMap.<String, String>of(), encoder);
    encoder.writeTo(indexOutput);
    sync(indexOutput);
  }

  /**
   * Writes the buffered data to underlying output stream.
   *
   * @param sync If {@code true}, perform a sync call to the underlying output stream.
   * @throws IOException If failed to flush.
   */
  private void flushBlock(boolean sync) throws IOException {
    if (encoder.size() == 0) {
      if (sync && !synced) {
        sync(eventOutput);
        sync(indexOutput);
        synced = true;
      }
      return;
    }

    // Record the current event output position if needs to update index
    long indexOffset = -1L;
    if (currentTimestamp >= nextIndexTime) {
      // Index offset is the current block start, hence is current position - 8 bytes timestamp already written.
      indexOffset = position - Bytes.SIZEOF_LONG;
    }

    // Writes the size of the encoded event
    lengthEncoder.writeInt(encoder.size());
    int size = lengthEncoder.size();
    lengthEncoder.writeTo(eventOutput);
    position += size;

    // Writes all encoded data from the buffer to the output.
    size = encoder.size();
    encoder.writeTo(eventOutput);
    position += size;
    if (sync) {
      sync(eventOutput);
    }

    if (indexOffset >= 0) {
      encoder.writeRaw(Bytes.toBytes(currentTimestamp));
      encoder.writeRaw(Bytes.toBytes(indexOffset));
      encoder.writeTo(indexOutput);
      if (sync) {
        sync(indexOutput);
      }

      nextIndexTime = currentTimestamp + indexInterval;
    } else if (sync) {
      sync(indexOutput);
    }

    // Reset the current timestamp so that a data block will start.
    currentTimestamp = -1L;
    synced = sync;
  }

  private void sync(OutputStream output) throws IOException {
    if (output instanceof Syncable) {
      ((Syncable) output).hsync();
    } else {
      output.flush();
    }
  }

  /**
   * Close this writer because of exception.
   * This method always throw exception.
   */
  private IOException closeWithException(IOException ex) throws IOException {
    closed = true;
    Closeables.closeQuietly(eventOutput);
    Closeables.closeQuietly(indexOutput);
    throw ex;
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
