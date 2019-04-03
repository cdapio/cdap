/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.logbuffer.recover;

import com.google.common.io.Closeables;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.logging.logbuffer.LogBufferEvent;
import io.cdap.cdap.logging.logbuffer.LogBufferFileOffset;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reader to read log buffer files.
 */
public class LogBufferReader implements Closeable {
  private static final String FILE_SUFFIX = ".buf";
  private final int batchSize;
  private final String baseDir;
  private final long maxFileId;

  private long currFileId;
  private LogBufferEventReader eventReader;
  private boolean skipFirstEvent;

  /**
   * Creates log buffer reader responsible for reading log buffer files.
   *
   * @param baseDir base directory for log buffer
   * @param batchSize max number of log events to read in one batch
   * @param maxFileId max file id to which recovery should happen
   * @param currFileId current log buffer file id
   * @param currPos position in current log buffer file
   * @throws IOException if there is any error while opening file to read
   */
  public LogBufferReader(String baseDir, int batchSize, long maxFileId, long currFileId,
                         long currPos) throws IOException {
    this.baseDir = baseDir;
    this.batchSize = batchSize;
    // if no checkpoints are written, currFileId and currPos will be -1. In that case the first event should not be
    // skipped. However, if currFileId and currPos are non negative, that means first event should be skipped as
    // atleast one log event has been persisted.
    this.currFileId = currFileId < 0 ? 0 : currFileId;
    this.skipFirstEvent = currFileId >= 0;
    this.maxFileId = maxFileId;
    this.eventReader = new LogBufferEventReader(baseDir, this.currFileId, currPos < 0 ? 0 : currPos);
  }

  /**
   * Reads next batch of events from log buffer.
   *
   * @param eventList events list to which events will be added
   * @return number of events added to eventList
   * @throws IOException error while reading events from buffer files
   */
  public int readEvents(List<LogBufferEvent> eventList) throws IOException {
    // there are no files in the log buffer directory. So return 0.
    if (maxFileId < 0) {
      return 0;
    }

    // iterate over all the remaining events.
    while (eventList.size() < batchSize && currFileId <= maxFileId) {
      try {
        if (eventReader == null) {
          eventReader = new LogBufferEventReader(baseDir, currFileId);
        }

        // skip the first event if skipFirstEvent is true. This is needed because log buffer offset represents offset
        // till which log events have been processed. Meaning current event is already processed by log buffer pipeline.
        if (skipFirstEvent) {
          eventReader.read();
          skipFirstEvent = false;
        }

        eventList.add(eventReader.read());
      } catch (FileNotFoundException e) {
        // move to next file in case file pointed by currFileId was not found
        currFileId++;
      } catch (EOFException e) {
        // reached eof on this event reader. So close it, move to next file
        eventReader.close();
        eventReader = null;
        currFileId++;
      }
    }

    return eventList.size();
  }

  @Override
  public void close() throws IOException {
    if (eventReader != null) {
      eventReader.close();
    }
  }



  /**
   * Log buffer event reader to read log events from a log buffer file.
   */
  private static final class LogBufferEventReader implements Closeable {
    private static final int BUFFER_SIZE = 32 * 1024; // 32k buffer
    private final DataInputStream inputStream;
    private final LoggingEventSerializer serializer;
    private long fileId;
    private long pos;

    LogBufferEventReader(String baseDir, long fileId) throws IOException {
      this(baseDir, fileId, 0);
    }

    LogBufferEventReader(String baseDir, long fileId, long pos) throws IOException {
      this.fileId = fileId;
      this.pos = pos;
      FileInputStream fis = new FileInputStream(new File(baseDir, fileId + FILE_SUFFIX));
      // seek to the position if the position is not zero
      if (pos != 0) {
        fis.getChannel().position(pos);
      }
      this.inputStream = new DataInputStream(new BufferedInputStream(fis, BUFFER_SIZE));
      this.serializer = new LoggingEventSerializer();
    }

    /**
     * Reads next event from log buffer file pointed by this reader.
     *
     * @return log buffer event
     * @throws IOException error while reading log buffer file
     */
    LogBufferEvent read() throws IOException {
      int length = inputStream.readInt();
      byte[] eventBytes = new byte[length];
      inputStream.read(eventBytes);
      LogBufferEvent event = new LogBufferEvent(serializer.fromBytes(ByteBuffer.wrap(eventBytes)),
                                                eventBytes.length, new LogBufferFileOffset(fileId, pos));
      // update curr position to point to next event
      pos = pos + Bytes.SIZEOF_INT + length;
      return event;
    }

    /**
     * Closes this reader.
     */
    public void close() {
      // close input stream wrapped by this reader
      Closeables.closeQuietly(inputStream);
    }
  }
}
