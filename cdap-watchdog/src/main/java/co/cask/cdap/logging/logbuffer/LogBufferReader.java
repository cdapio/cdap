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
 * Reads logs from log buffer provided initial start file and start position.
 */
public class LogBufferReader implements Closeable {
  private static final String FILE_SUFFIX = ".buf";
  private final LoggingEventSerializer serializer;
  private final int batchSize;
  private final String baseDir;

  private long currFileId;
  private long currPos;
  private DataInputStream currInputStream;
  private boolean skipFirstEvent;

  LogBufferReader(String baseDir, int batchSize, long currFileId, long currPos) {
    this.baseDir = baseDir;
    this.batchSize = batchSize;
    this.currFileId = currFileId;
    this.currPos = currPos;
    this.skipFirstEvent = true;
    this.serializer = new LoggingEventSerializer();
  }

  /**
   * Reads next batch of events from log buffer.
   *
   * @param eventList
   * @return
   */
  int readEvents(List<LogBufferEvent> eventList) throws IOException {

    // initialize the input stream and skip the first event
    if (skipFirstEvent) {
      if (currInputStream == null) {
        FileInputStream fis = new FileInputStream(new File(baseDir, currFileId + FILE_SUFFIX));
        // seek to start file and position
        fis.getChannel().position(currPos);
        currInputStream = new DataInputStream(fis);
      }

      // skip the event
      int length = currInputStream.readInt();
      currInputStream.skipBytes(length);
      // update curr position to point to next event
      currPos = currPos + Bytes.SIZEOF_INT + length;
      skipFirstEvent = false;
    }

    // iterate over all the remaining events
    while (eventList.size() < batchSize) {
      try {
        int length = currInputStream.readInt();
        // read event
        byte[] eventBytes = new byte[length];
        currInputStream.read(eventBytes);
        // add to eventList
        eventList.add(new LogBufferEvent(serializer.fromBytes(ByteBuffer.wrap(eventBytes)),
                                         eventBytes.length, new LogBufferFileOffset(currFileId, currPos)));
        // update curr position to point to next event
        currPos = currPos + Bytes.SIZEOF_INT + length;
      } catch (EOFException e) {
        // close current input stream
        Closeables.closeQuietly(currInputStream);
        // open new file input stream
        currInputStream = new DataInputStream(new FileInputStream(new File(baseDir, currFileId + FILE_SUFFIX)));
        // increment currFileId and reset currPos
        currFileId++;
        currPos = 0;
      }
    }

    return eventList.size();
  }

  @Override
  public void close() throws IOException {
    if (currInputStream != null) {
      Closeables.closeQuietly(currInputStream);
    }
  }
}
