/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.system;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.WarnStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.logging.AppenderContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Syncable;
import io.cdap.cdap.logging.clean.FileMetadataCleaner;
import io.cdap.cdap.logging.clean.LogCleaner;
import io.cdap.cdap.logging.meta.FileMetaDataWriter;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Log Appender implementation for CDAP Log framework
 * TODO : Refactor package CDAP-8196
 */
public class CDAPLogAppender extends AppenderBase<ILoggingEvent> implements Flushable, Syncable {
  private static final Logger LOG = LoggerFactory.getLogger(CDAPLogAppender.class);
  private static final Set<String> PROGRAM_ID_KEYS = ImmutableSet.of(Constants.Logging.TAG_MAP_REDUCE_JOB_ID,
                                                                     Constants.Logging.TAG_SPARK_JOB_ID,
                                                                     Constants.Logging.TAG_USER_SERVICE_ID,
                                                                     Constants.Logging.TAG_WORKER_ID,
                                                                     Constants.Logging.TAG_WORKFLOW_ID);
  private LogFileManager logFileManager;

  private String dirPermissions;
  private String filePermissions;
  private int syncIntervalBytes;
  private long maxFileLifetimeMs;
  private long maxFileSizeInBytes;
  private ScheduledExecutorService scheduledExecutorService;
  private int logCleanupIntervalMins;
  private int fileRetentionDurationDays;
  private int fileCleanupBatchSize;

  public CDAPLogAppender() {
    setName(getClass().getName());
  }

  /**
   * Sets the permissions for directory created by this appender. This is called by the logback framework.
   */
  public void setDirPermissions(String dirPermissions) {
    this.dirPermissions = dirPermissions;
  }

  /**
   * Sets the permissions for locations created by this appender. This is called by the logback framework.
   */
  public void setFilePermissions(String filePermissions) {
    this.filePermissions = filePermissions;
  }

  /**
   * Sets the avro file sync interval. This is called by the logback framework.
   */
  public void setSyncIntervalBytes(int syncIntervalBytes) {
    this.syncIntervalBytes = syncIntervalBytes;
  }

  /**
   * Sets the maximum lifetime of a file. This is called by the logback framework.
   */
  public void setMaxFileLifetimeMs(long maxFileLifetimeMs) {
    this.maxFileLifetimeMs = maxFileLifetimeMs;
  }

  /**
   * Sets the maximum size of a file. This is called by the logback framework.
   */
  public void setMaxFileSizeInBytes(long maxFileSizeInBytes) {
    this.maxFileSizeInBytes = maxFileSizeInBytes;
  }

  /**
   * Sets the file retention duration for the file,
   * after this duration the file gets cleaned up by log clean up thread.
   */
  public void setFileRetentionDurationDays(int fileRetentionDurationDays) {
    this.fileRetentionDurationDays = fileRetentionDurationDays;
  }

  /**
   * Sets the log cleanup interval
   */
  public void setLogCleanupIntervalMins(int logCleanupIntervalMins) {
    this.logCleanupIntervalMins = logCleanupIntervalMins;
  }

  /**
   * Sets batch size for file cleanup
   */
  public void setFileCleanupBatchSize(int batchSize) {
    this.fileCleanupBatchSize = batchSize;
  }


  @Override
  public void start() {
    // These should all passed. The settings are from the cdap-log-pipeline.xml and the context must be AppenderContext
    Preconditions.checkState(dirPermissions != null, "Property dirPermissions cannot be null");
    Preconditions.checkState(filePermissions != null, "Property filePermissions cannot be null");
    Preconditions.checkState(syncIntervalBytes > 0, "Property syncIntervalBytes must be > 0.");
    Preconditions.checkState(maxFileLifetimeMs > 0, "Property maxFileLifetimeMs must be > 0");
    Preconditions.checkState(maxFileSizeInBytes > 0, "Property maxFileSizeInBytes must be > 0");
    Preconditions.checkState(fileRetentionDurationDays > 0, "Property fileRetentionDurationDays must be > 0");
    Preconditions.checkState(logCleanupIntervalMins > 0, "Property logCleanupIntervalMins must be > 0");
    Preconditions.checkState(fileCleanupBatchSize > 0, "Property fileCleanupBatchSize must be > 0");

    if (context instanceof AppenderContext) {
      AppenderContext context = (AppenderContext) this.context;
      logFileManager = new LogFileManager(dirPermissions, filePermissions, maxFileLifetimeMs, maxFileSizeInBytes,
                                          syncIntervalBytes,
                                          new FileMetaDataWriter(context.getTransactionRunner()),
                                          context.getLocationFactory());
      if (context.getInstanceId() == 0) {
        scheduledExecutorService =
          Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("log-clean-up"));
        FileMetadataCleaner fileMetadataCleaner = new FileMetadataCleaner(context.getTransactionRunner());
        LogCleaner logCleaner = new LogCleaner(fileMetadataCleaner, context.getLocationFactory(),
                                               TimeUnit.DAYS.toMillis(fileRetentionDurationDays),
                                               fileCleanupBatchSize);
        scheduledExecutorService.scheduleAtFixedRate(logCleaner, 10, logCleanupIntervalMins, TimeUnit.MINUTES);
      }
    } else if (!Boolean.TRUE.equals(context.getObject(Constants.Logging.PIPELINE_VALIDATION))) {
      throw new IllegalStateException("Expected logger context instance of " + AppenderContext.class.getName() +
                                        " but get " + context.getClass().getName());
    }
    super.start();
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) throws LogbackException {
    if (logFileManager == null) {
      return;
    }

    long timestamp = eventObject.getTimeStamp();
    try {
      // logic from AppenderBase
      if (!this.started) {
        addStatus(new WarnStatus(
          "Attempted to append to non started appender [" + name + "].",
          this));
        return;
      }

      // logic from AppenderBase
      if (getFilterChainDecision(eventObject) == FilterReply.DENY) {
        return;
      }

      LogPathIdentifier logPathIdentifier = getLoggingPath(eventObject.getMDCPropertyMap());
      LogFileOutputStream outputStream = logFileManager.getLogFileOutputStream(logPathIdentifier, timestamp);
      outputStream.append(eventObject);
    } catch (IllegalArgumentException iae) {
      // this shouldn't happen
      LOG.error("Unrecognized context ", iae);
    } catch (IOException ioe) {
      throw new LogbackException("Exception during append", ioe);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    // no-op - this wont be called as we are overriding doAppend
  }

  @Override
  public void flush() throws IOException {
    if (logFileManager != null) {
      logFileManager.flush();
    }
  }

  @Override
  public void sync() throws IOException {
    if (logFileManager != null) {
      logFileManager.sync();
    }
  }

  @Override
  public void stop() {
    try {
      if (logFileManager != null) {
        try {
          logFileManager.close();
        } catch (IOException e) {
          //TODO
        }
      }
      scheduledExecutorService.shutdownNow();
    } finally {
      super.stop();
    }
  }

  @VisibleForTesting
  LogPathIdentifier getLoggingPath(Map<String, String> propertyMap) throws IllegalArgumentException {
    // from the property map, get namespace values
    // if the namespace is system : get component-id and return that as path
    // if the namespace is non-system : get "app" and "program-name" and return that as path

    String namespaceId = propertyMap.get(Constants.Logging.TAG_NAMESPACE_ID);

    // If logs are from applications in system namespace,
    // their path will include application and program similar to other namespaces,
    // else the path will include component and service
    if (NamespaceId.SYSTEM.getNamespace().equals(namespaceId)
      && !propertyMap.containsKey(Constants.Logging.TAG_APPLICATION_ID)) {
      Preconditions.checkArgument(propertyMap.containsKey(Constants.Logging.TAG_SERVICE_ID),
                                  "%s is expected but not found in the context %s",
                                  Constants.Logging.TAG_SERVICE_ID, propertyMap);
      // adding services to be consistent with the old format
      return new LogPathIdentifier(namespaceId, Constants.Logging.COMPONENT_NAME,
                                   propertyMap.get(Constants.Logging.TAG_SERVICE_ID));
    } else {
      Preconditions.checkArgument(propertyMap.containsKey(Constants.Logging.TAG_APPLICATION_ID),
                                  "%s is expected but not found in the context %s",
                                  Constants.Logging.TAG_APPLICATION_ID, propertyMap);
      String application = propertyMap.get(Constants.Logging.TAG_APPLICATION_ID);

      String program = null;
      for (String programId : PROGRAM_ID_KEYS) {
        if (propertyMap.containsKey(programId)) {
          program = propertyMap.get(programId);
          break;
        }
      }
      Preconditions.checkArgument(program != null, String.format("Unrecognized program in the context %s",
                                                                 propertyMap));
      return new LogPathIdentifier(namespaceId, application, program);
    }
  }
}
