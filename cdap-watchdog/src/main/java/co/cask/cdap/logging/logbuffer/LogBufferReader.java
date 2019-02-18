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

package co.cask.cdap.logging.logbuffer;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.io.Closeables;

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
  private LogBufferInputStream inputStream;
  private boolean skipFirstEvent;

  public LogBufferReader(String baseDir, int batchSize, long currFileId, long currPos) throws IOException {
    this.baseDir = baseDir;
    this.batchSize = batchSize;
    // if no checkpoints are written, start from 0th position from first file. Also in that case do not skip first event
    this.currFileId = currFileId < 0 ? 0 : currFileId;
    this.skipFirstEvent = currFileId >= 0;
    this.maxFileId = getMaxFileId(baseDir);
    this.inputStream = new LogBufferInputStream(baseDir, this.currFileId, currPos < 0 ? 0 : currPos);
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
    while (eventList.size() < batchSize && currFileId < (maxFileId + 1)) {
      try {
        if (inputStream == null) {
          inputStream = new LogBufferInputStream(baseDir, currFileId);
        }

        // skip the first event if skipFirstEvent is true. This is needed because log buffer offset represents offset
        // till which log events have been processed. Meaning current event is already processed by log buffer pipeline.
        if (skipFirstEvent) {
          inputStream.read();
          skipFirstEvent = false;
        }

        eventList.add(inputStream.read());
      } catch (FileNotFoundException e) {
        // move to next file in case file pointed by currFileId was not found
        currFileId++;
      } catch (EOFException e) {
        // reached eof on this input stream. So close it, move to next file
        inputStream.close();
        inputStream = null;
        currFileId++;
      }
    }

    return eventList.size();
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
    }
  }

  private long getMaxFileId(String baseDir) {
    long maxFileId = -1;
    File[] files = new File(baseDir).listFiles();
    if (files != null) {
      for (File file : files) {
        String[] splitted = file.getName().split("\\.");
        long fileId = Long.parseLong(splitted[0]);
        if (maxFileId < fileId) {
          maxFileId = fileId;
        }
      }
    }

    return maxFileId;
  }

  /**
   * Input stream to interact with log buffer files.
   */
  private static final class LogBufferInputStream {
    private long fileId;
    private long pos;
    private DataInputStream inputStream;
    private final LoggingEventSerializer serializer;

    LogBufferInputStream(String baseDir, long fileId) throws IOException {
      this(baseDir, fileId, 0);
    }

    LogBufferInputStream(String baseDir, long fileId, long pos) throws IOException {
      this.fileId = fileId;
      this.pos = pos;
      FileInputStream fis = new FileInputStream(new File(baseDir, fileId + FILE_SUFFIX));
      // seek to the position if the position is not zero
      if (pos != 0) {
        fis.getChannel().position(pos);
      }
      this.inputStream = new DataInputStream(fis);
      this.serializer = new LoggingEventSerializer();
    }

    /**
     * Reads next event from log buffer file pointed by this input stream.
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
     * Closes this input stream.
     */
    void close() {
      // close current input stream
      Closeables.closeQuietly(inputStream);
    }
  }
}
