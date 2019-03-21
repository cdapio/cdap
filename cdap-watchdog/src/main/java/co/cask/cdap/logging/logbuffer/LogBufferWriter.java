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
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Appends logs to log buffer file. The file is rotated when it reaches max size. The log buffer file name is
 * monotonically increasing number. When a new file is created, the file name becomes (max_file_id + 1).buf. The
 * file format is as below
 *
 * <length> <log_event>
 * length = Avro encoded int32 for size in bytes for the log event
 * log_event = Avro encoded log bytes
 */
public class LogBufferWriter implements Flushable, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LogBufferWriter.class);
  private static final String FILE_SUFFIX = ".buf";
  private final LoggingEventSerializer logEventSerializer;
  private final LocationFactory locationFactory;
  private final long maxFileSizeInBytes;
  private final Runnable cleaner;
  private final ExecutorService executorService;
  private Future<?> cleanerFuture;

  // output stream to write to
  private OutputStream currOutputStream;
  // offset in current open file
  private long currOffset;
  // cache to store monotonically increasing id for file id
  private long currFileId;
  private long writtenBytes;

  public LogBufferWriter(String logBufferBaseDir, long maxFileSize, Runnable cleaner) throws IOException {
    File baseDir = new File(logBufferBaseDir);
    // make sure base dir already exists, if not create it.
    Files.createDirectories(baseDir.toPath());
    this.locationFactory = new LocalLocationFactory(baseDir);
    // max file size after which rotation should happen.
    this.maxFileSizeInBytes = maxFileSize;
    this.cleaner = cleaner;
    // Mark cleaner future as completed when its initialized
    this.cleanerFuture =  CompletableFuture.completedFuture(0);
    this.executorService = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("log-buffer-cleaner"));
    this.logEventSerializer = new LoggingEventSerializer();

    // scan file names under base dir and get next monotonically increasing file id
    this.currFileId = getNextFileId(baseDir);
    this.currOutputStream = new BufferedOutputStream(locationFactory.create(getFileName(currFileId)).getOutputStream());
  }

  /**
   * Write events to log buffer.
   *
   * @param events iterator of events to be written
   * @return iterator of log buffer file offsets
   * @throws IOException if there is any problem while writing to log buffer
   */
  public Iterable<LogBufferEvent> write(Iterator<byte[]> events) throws IOException {
    List<LogBufferEvent> offsets = new LinkedList<>();
    while (events.hasNext()) {
      byte[] event = events.next();
      LogBufferFileOffset offset = write(event);
      offsets.add(new LogBufferEvent(logEventSerializer.fromBytes(ByteBuffer.wrap(event)), event.length, offset));
    }
    currOutputStream.flush();
    return offsets;
  }

  /**
   * Writes an event to log buffer file. If the buffer file has reached its max size limit, new file is created with
   * monotonically increasing file name.
   *
   * @param eventBytes event to be written to log buffer
   * @return log buffer file offset
   * @throws IOException if there is any problem while writing to log buffer
   */
  private LogBufferFileOffset write(byte[] eventBytes) throws IOException {
    long startFileId = currFileId;
    long startOffset = currOffset;

    // write size of the log event
    currOutputStream.write(Bytes.toBytes(eventBytes.length));
    currOffset = currOffset + Bytes.SIZEOF_INT;
    // write actual log event
    currOutputStream.write(eventBytes);
    currOffset = currOffset + eventBytes.length;

    // update number of bytes written so far
    writtenBytes = writtenBytes + (currOffset - startOffset);

    // If number of written bytes exceed the max file size, then rotate the file
    if (writtenBytes >= maxFileSizeInBytes) {
      currOutputStream = new BufferedOutputStream(rotateFile(currOutputStream).getOutputStream());
    }

    // the file id and file pos in offset is where current event is written.
    return new LogBufferFileOffset(startFileId, startOffset);
  }

  @Override
  public void flush() throws IOException {
    currOutputStream.flush();
  }

  @Override
  public void close() throws IOException {
    try {
      currOutputStream.flush();
    } catch (IOException e) {
      LOG.warn("Error while flushing log buffer output stream.", e);
    }

    Closeables.closeQuietly(currOutputStream);
    executorService.shutdown();
  }

  /**
   * Returns next monotonically increasing file id. The method scans all the files under base path and returns
   * max file id + 1.
   */
  private long getNextFileId(File baseDir) {
    long maxFileId = -1;
    File[] files = baseDir.listFiles();
    if (files != null) {
      for (File file : files) {
        String[] splitted = file.getName().split("\\.");
        long fileId = Long.parseLong(splitted[0]);
        if (maxFileId < fileId) {
          maxFileId = fileId;
        }
      }
    }

    return maxFileId + 1;
  }

  /**
   * Rotates the log buffer file.
   */
  private Location rotateFile(OutputStream currOutputStream) throws IOException {
    currOutputStream.flush();
    // close current location output stream
    Closeables.closeQuietly(currOutputStream);

    writtenBytes = 0;
    currOffset = 0;
    // update current file id to next monotonically increasing file id
    currFileId = currFileId + 1;
    Location rotatedLocation = locationFactory.create(getFileName(currFileId));
    // executes log buffer cleaner runnable. Only submit cleaner thread if future is complete. This is because if the
    // rotation is happening faster than clean up, there can be multiple clean up tasks in executorService.
    if (cleanerFuture.isDone()) {
      cleanerFuture = executorService.submit(cleaner);
    }
    return rotatedLocation;
  }

  private String getFileName(long fileId) {
    return fileId + FILE_SUFFIX;
  }
}
