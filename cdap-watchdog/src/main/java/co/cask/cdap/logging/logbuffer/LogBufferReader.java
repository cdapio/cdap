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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reader to read log buffer files.
 */
public class LogBufferReader implements Closeable {
  private static final String FILE_SUFFIX = ".buf";
  private final LoggingEventSerializer serializer;
  private final int batchSize;
  private final String baseDir;
  private final long maxFileId;

  private long currFileId;
  private long currPos;
  private DataInputStream currInputStream;
  private boolean skipFirstEvent;

  public LogBufferReader(String baseDir, int batchSize, long currFileId, long currPos) {
    this.baseDir = baseDir;
    this.batchSize = batchSize;
    // if no checkpoints are written, start from 0th position from first file. Also in that case do not skip first event
    this.currFileId = currFileId < 0 ? 0 : currFileId;
    this.currPos = currPos < 0 ? 0 : currPos;
    this.skipFirstEvent = currFileId >= 0;
    this.maxFileId = getMaxFileId(baseDir);
    this.serializer = new LoggingEventSerializer();
  }

  /**
   * Reads next batch of events from log buffer.
   *
   * @param eventList events list to which events will be added
   * @return number of events added to eventList
   * @throws IOException error while reading events from buffer files
   * @return number of events read
   */
  public int readEvents(List<LogBufferEvent> eventList) throws IOException {
    // there are no files in the log buffer directory. So return 0.
    if (maxFileId < 0) {
      return 0;
    }

    // initialize the input stream and skip the first event if skipFirstEvent is true. This is needed because log
    // buffer offset represent offset till which log events have been processed. Meaning current event is already
    // processed. So skip it.
    skipFirstIfNeeded();

    // iterate over all the remaining events.
    int count = 0;
    while (count < batchSize && currFileId < (maxFileId + 1)) {
      try {
        if (currInputStream == null) {
          File file = new File(baseDir, currFileId + FILE_SUFFIX);
          if (!file.exists()) {
            // if file does not exist, reset and move to next file
            reset();
            continue;
          }
          currInputStream = new DataInputStream(new FileInputStream(file));
        }
        eventList.add(readEvent());
        count++;
      } catch (EOFException e) {
        // if we reached end of file, then reset and move to next file
        reset();
      }
    }

    return count;
  }

  @Override
  public void close() throws IOException {
    if (currInputStream != null) {
      Closeables.closeQuietly(currInputStream);
    }
  }

  private void skipFirstIfNeeded() throws IOException {
    if (skipFirstEvent) {
      FileInputStream fis = new FileInputStream(new File(baseDir, currFileId + FILE_SUFFIX));
      // seek to start file and position
      fis.getChannel().position(currPos);
      currInputStream = new DataInputStream(fis);
      // skip the event
      readEvent();
      skipFirstEvent = false;
    }
  }

  private LogBufferEvent readEvent() throws IOException {
    int length = currInputStream.readInt();
    byte[] eventBytes = new byte[length];
    currInputStream.read(eventBytes);
    LogBufferEvent event = new LogBufferEvent(serializer.fromBytes(ByteBuffer.wrap(eventBytes)),
                              eventBytes.length, new LogBufferFileOffset(currFileId, currPos));
    // update curr position to point to next event
    currPos = currPos + Bytes.SIZEOF_INT + length;
    return event;
  }

  private void reset() {
    // close current input stream
    Closeables.closeQuietly(currInputStream);
    // Reset the input stream state
    currInputStream = null;
    currFileId++;
    currPos = 0;
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
}
