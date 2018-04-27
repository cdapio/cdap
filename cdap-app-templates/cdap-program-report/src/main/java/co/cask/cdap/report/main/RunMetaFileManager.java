/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.main;

import com.google.common.io.Closeables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Maintains map of namespace to output stream and handles operations to add, flush, sync to stream and closing
 * during cleanup. During adding to file, if the file has to be rotated based on either (time limit exceeded) or
 * file size exceeded, we will rotate to new file,
 * close the old one and maintain the reference to the new file in the map
 */
public class RunMetaFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(RunMetaFileManager.class);
  private static final Integer DEFAULT_MAX_FILE_SIZE_BYTES = 67108864;
  private static final Integer DEFAULT_SYNC_INTERVAL_BYTES = 10485760;
  private static final Long DEFAULT_MAX_FILE_OPEN_DURATION = TimeUnit.HOURS.toMillis(6);
  private static final String SYNC_INTERVAL = "file.sync.interval.bytes";
  private static final String MAX_FILE_SIZE_BYTES = "file.max.size.bytes";
  private static final String MAX_FILE_OPEN_DURATION = "file.max.open.duration";

  private final int syncIntervalBytes;
  private final int maxFileSizeBytes;
  private final long maxFileOpenDuration;

  private Map<String, RunMetaFileOutputStream> namespaceToLogFileStreamMap;
  private Location baseLocation;

  RunMetaFileManager(Location baseLocation, Map<String, String> runtimeArguments) {
    this.namespaceToLogFileStreamMap = new HashMap<>();
    this.baseLocation = baseLocation;
    this.syncIntervalBytes = runtimeArguments.containsKey(SYNC_INTERVAL) ?
      Integer.parseInt(runtimeArguments.get(SYNC_INTERVAL)) : DEFAULT_SYNC_INTERVAL_BYTES;
    this.maxFileSizeBytes = runtimeArguments.containsKey(MAX_FILE_SIZE_BYTES) ?
      Integer.parseInt(runtimeArguments.get(MAX_FILE_SIZE_BYTES)) : DEFAULT_MAX_FILE_SIZE_BYTES;
    this.maxFileOpenDuration = runtimeArguments.containsKey(MAX_FILE_OPEN_DURATION) ?
      Integer.parseInt(runtimeArguments.get(MAX_FILE_OPEN_DURATION)) : DEFAULT_MAX_FILE_OPEN_DURATION;
  }

  /**
   * append {@link ProgramRunInfo} and flush to file. create or rotate file if needed before appending.
   * @param programRunInfo
   * @throws InterruptedException
   */
  public void append(ProgramRunInfo programRunInfo) throws InterruptedException {
    if (!namespaceToLogFileStreamMap.containsKey(programRunInfo.getNamespace())) {
      // create a output stream if file doesnt exist already for this namespace in the map
      createLogFileOutputStreamWithRetry(programRunInfo.getNamespace(),
                                         programRunInfo.getTimestamp());
    }
    rotateOutputStreamIfNeeded(namespaceToLogFileStreamMap.get(programRunInfo.getNamespace()),
                               programRunInfo.getNamespace(), programRunInfo.getTimestamp());
    RunMetaFileOutputStream outputStream = namespaceToLogFileStreamMap.get(programRunInfo.getNamespace());
    appendAndFlushWithRetry(outputStream, programRunInfo);
  }

  /**
   * sync the open output streams, hdfs sync is called
   * @throws InterruptedException
   */
  public void syncOutputStreams() throws InterruptedException {
    Collection<RunMetaFileOutputStream> outputStreams = namespaceToLogFileStreamMap.values();
    for (RunMetaFileOutputStream outputStream : outputStreams) {
      retryWithCallable(() -> outputStream.sync(), "sync");
    }
  }

  public void cleanup() {
    Collection<RunMetaFileOutputStream> outputStreams = namespaceToLogFileStreamMap.values();
    for (RunMetaFileOutputStream outputStream : outputStreams) {
      Closeables.closeQuietly(outputStream);
    }
  }

  private void createLogFileOutputStreamWithRetry(String namespace, Long timestamp) throws InterruptedException {
    while (!createLogFileOutputStream(namespace, timestamp)) {
      LOG.warn("Failed to create log file for the namespace {} and timestamp {}", namespace, timestamp);
      TimeUnit.MILLISECONDS.sleep(10);
    }
    LOG.info("Successfully created log file for the namespace {} and timestamp {}", namespace, timestamp);
  }

  private boolean createLogFileOutputStream(String namespace, Long timestamp) {
    try {
      Location namespaceDir = getOrCreateAndGet(namespace);
      Location fileLocation;
      String fileName = String.format("%s-%s.avro", timestamp, System.currentTimeMillis());
      fileLocation = namespaceDir.append(fileName);
      boolean successful = fileLocation.createNew();
      if (successful) {
        namespaceToLogFileStreamMap.put(namespace,
                                        new RunMetaFileOutputStream(fileLocation, "", syncIntervalBytes,
                                                                    System.currentTimeMillis(), () ->
                                                                      namespaceToLogFileStreamMap.remove(namespace)));
      }
      return successful;
    } catch (IOException e) {
      LOG.warn("Exception while trying to create file location ", e);
      return false;
    }
  }

  private void rotateOutputStreamIfNeeded(RunMetaFileOutputStream runMetaFileOutputStream,
                                          String namespace, Long timestamp) throws InterruptedException {
    if (runMetaFileOutputStream.getSize() > maxFileSizeBytes ||
      System.currentTimeMillis() - runMetaFileOutputStream.getCreateTime() > maxFileOpenDuration) {
      Closeables.closeQuietly(runMetaFileOutputStream);
      createLogFileOutputStreamWithRetry(namespace, timestamp);
    }
  }

  private Location getOrCreateAndGet(String namespace) throws IOException {
    List<Location> namespaces = baseLocation.list();
    for (Location location : namespaces) {
      if (location.getName().equals(namespace)) {
        return location;
      }
    }
    Location namespaceLocation = baseLocation.append(namespace);
    namespaceLocation.mkdirs();
    return namespaceLocation;
  }

  private void appendAndFlushWithRetry(RunMetaFileOutputStream outputStream,
                                       ProgramRunInfo programRunInfo) throws InterruptedException {
    retryWithCallable(() -> outputStream.append(programRunInfo), "append");
    retryWithCallable(() -> outputStream.flush(), "flush");
  }

  private void retryWithCallable(Callable callable, String operation) throws InterruptedException {
    SampledLogging sampledLogging = new SampledLogging(LOG, 100);
    boolean success = false;
    while (!success) {
      try {
        callable.call();
        success = true;
      } catch (IOException e) {
        sampledLogging.logWarning(String.format("Exception while performing %s : ", operation), e);
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
  }

  /**
   * callable throwing IOException
   */
  private interface Callable {
    void call() throws IOException;
  }
}
