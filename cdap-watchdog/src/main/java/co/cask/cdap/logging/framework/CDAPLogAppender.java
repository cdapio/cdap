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
import ch.qos.logback.core.LogbackException;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
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

  private LogFileManager logFileManager;

  @Inject
  private FileMetaDataManager fileMetaDataManager;
  @Inject
  private LocationFactory locationFactory;

  private int syncIntervalBytes;
  private long maxFileLifetimeMs;


  /**
   * TODO: start a separate cleanup thread to remove files that has passed the TTL
   */
  public CDAPLogAppender() {
    setName(getClass().getName());
  }

  public void setSyncIntervalBytes(int syncIntervalBytes) {
    this.syncIntervalBytes = syncIntervalBytes;
  }

  public void setMaxFileLifetimeMs(long maxFileLifetimeMs) {
    this.maxFileLifetimeMs = maxFileLifetimeMs;
  }

  @Override
  public void start() {
    super.start();
    try {
      this.logFileManager = new LogFileManager(maxFileLifetimeMs, syncIntervalBytes, new LogSchema().getAvroSchema(),
                                               fileMetaDataManager, locationFactory);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void doAppend(ILoggingEvent eventObject) throws LogbackException {
    long timestamp = eventObject.getTimeStamp();
    LogPathIdentifier logPathIdentifier = LoggingUtil.getLoggingPath(eventObject.getMDCPropertyMap());
    LogFileOutputStream outputStream;

    try {
      outputStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }

    try {
      outputStream.append(eventObject);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void append(ILoggingEvent eventObject) {
    long timestamp = eventObject.getTimeStamp();
    LogPathIdentifier logPathIdentifier = LoggingUtil.getLoggingPath(eventObject.getMDCPropertyMap());
    LogFileOutputStream outputStream;

    try {
      outputStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }

    try {
      outputStream.append(eventObject);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void flush() throws IOException {
    logFileManager.flush();
  }

  @Override
  public void stop() {
    try {
      logFileManager.close();
    } finally {
      super.stop();
    }
  }
}
