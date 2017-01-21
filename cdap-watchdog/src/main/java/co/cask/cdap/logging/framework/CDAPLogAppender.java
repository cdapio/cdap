/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;

/**
 * Log Appender implementation for CDAP Log framework
 */
public class CDAPLogAppender extends AppenderBase<ILoggingEvent> implements Flushable {
  private static final Logger LOG = LoggerFactory.getLogger(CDAPLogAppender.class);

  private boolean isStarted;
  private final AvroFileManager avroFileManager;

  /**
   * TODO: start a separate cleanup thread to remove files that has passed the TTL
   */

  /**
   * Constructs an AvroFileWriter object.
   * @param fileMetaDataManager used to store file meta data.
   * @param locationFactory the location factory
   * @param schema schema of the Avro data to be written.
   * @param syncIntervalBytes the approximate number of uncompressed bytes to write in each block.
   * @param maxFileLifetimeMs files that are older than maxFileLifetimeMs will be closed.
   */
  public CDAPLogAppender(FileMetaDataManager fileMetaDataManager, LocationFactory locationFactory,
                         Schema schema, int syncIntervalBytes, long maxFileLifetimeMs) {
    this.avroFileManager = new AvroFileManager(maxFileLifetimeMs, syncIntervalBytes,
                                               schema, fileMetaDataManager, locationFactory);
    this.isStarted = false;
  }

  public void append(ILoggingEvent eventObject) {
    long timestamp = eventObject.getTimeStamp();

    LogPathIdentifier logPathIdentifier = LoggingUtil.getLoggingPath(eventObject.getMDCPropertyMap());
    AvroFileManager.AvroFile avroFile;
    try {
      avroFile = avroFileManager.getAvroFile(logPathIdentifier, timestamp);
    } catch (IOException ioe) {
      // if there's exception while creating the file, we keep retrying
      LOG.debug("Exception while creating avro file", ioe);
      throw Throwables.propagate(ioe);
    }
    try {
      avroFile.append(eventObject);
    } catch (Exception e) {
      LOG.trace("Exception while appending to file", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void flush() throws IOException {
    avroFileManager.flush();
  }

  @Override
  public void stop() {
    avroFileManager.close();
    this.isStarted = false;
  }

  @Override
  public boolean isStarted() {
    return isStarted;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

}
