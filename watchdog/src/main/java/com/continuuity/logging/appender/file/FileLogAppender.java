/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.file;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppender;
import com.continuuity.logging.save.LogSaver;
import com.continuuity.logging.save.LogSaverTableUtil;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.logging.serialize.LoggingEvent;
import com.continuuity.logging.write.AvroFileWriter;
import com.continuuity.logging.write.FileMetaDataManager;
import com.continuuity.logging.write.LogCleanup;
import com.continuuity.logging.write.LogFileWriter;
import com.continuuity.logging.write.LogWriteEvent;
import com.continuuity.logging.write.SimpleLogFileWriter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logback appender that writes log events to files.
 */
public class FileLogAppender extends LogAppender {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogAppender.class);

  public static final String APPENDER_NAME = "FileLogAppender";

  private final LogSaverTableUtil tableUtil;
  private final TransactionSystemClient txClient;
  private final LocationFactory locationFactory;
  private final Location logBaseDir;
  private final int syncIntervalBytes;
  private final long retentionDurationMs;
  private final long maxLogFileSizeBytes;
  private final long inactiveIntervalMs;
  private final long checkpointIntervalMs;
  private final int logCleanupIntervalMins;
  private final ListeningScheduledExecutorService scheduledExecutor;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private LogFileWriter<LogWriteEvent> logFileWriter;
  private Schema logSchema;

  @Inject
  public FileLogAppender(CConfiguration cConfig, DataSetAccessor dataSetAccessor,
                         TransactionSystemClient txClient,
                         LocationFactory locationFactory) {
    setName(APPENDER_NAME);

    this.tableUtil = new LogSaverTableUtil(dataSetAccessor);
    this.txClient = txClient;
    this.locationFactory = locationFactory;

    String baseDir = cConfig.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    this.logBaseDir = locationFactory.create(baseDir);

    this.syncIntervalBytes = cConfig.getInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 50 * 1024);
    Preconditions.checkArgument(this.syncIntervalBytes > 0,
                                "Log file sync interval is invalid: %s", this.syncIntervalBytes);

    long retentionDurationDays = cConfig.getLong(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS, -1);
    Preconditions.checkArgument(retentionDurationDays > 0,
                                "Log file retention duration is invalid: %s", retentionDurationDays);
    this.retentionDurationMs = TimeUnit.MILLISECONDS.convert(retentionDurationDays, TimeUnit.DAYS);

    maxLogFileSizeBytes = cConfig.getLong(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024 * 1024);
    Preconditions.checkArgument(maxLogFileSizeBytes > 0,
                                "Max log file size is invalid: %s", maxLogFileSizeBytes);

    inactiveIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_INACTIVE_FILE_INTERVAL_MS,
                                              LoggingConfiguration.DEFAULT_LOG_SAVER_INACTIVE_FILE_INTERVAL_MS);
    Preconditions.checkArgument(inactiveIntervalMs > 0,
                                "Inactive interval is invalid: %s", inactiveIntervalMs);

    checkpointIntervalMs = cConfig.getLong(LoggingConfiguration.LOG_SAVER_CHECKPOINT_INTERVAL_MS,
                                                LoggingConfiguration.DEFAULT_LOG_SAVER_CHECKPOINT_INTERVAL_MS);
    Preconditions.checkArgument(checkpointIntervalMs > 0,
                                "Checkpoint interval is invalid: %s", checkpointIntervalMs);

    logCleanupIntervalMins = cConfig.getInt(LoggingConfiguration.LOG_CLEANUP_RUN_INTERVAL_MINS,
                                            LoggingConfiguration.DEFAULT_LOG_CLEANUP_RUN_INTERVAL_MINS);
    Preconditions.checkArgument(logCleanupIntervalMins > 0,
                                "Log cleanup run interval is invalid: %s", logCleanupIntervalMins);

    this.scheduledExecutor =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("file-log-appender")));
  }

  @Override
  public void start() {
    super.start();
    try {
      logSchema = new LogSchema().getAvroSchema();
      FileMetaDataManager fileMetaDataManager = new FileMetaDataManager(tableUtil.getMetaTable(),
                                                                        txClient,
                                                                        locationFactory);

      AvroFileWriter avroFileWriter = new AvroFileWriter(fileMetaDataManager, logBaseDir,
                                                         logSchema,
                                                         maxLogFileSizeBytes, syncIntervalBytes,
                                                         inactiveIntervalMs);
      logFileWriter = new SimpleLogFileWriter(avroFileWriter, checkpointIntervalMs);

      LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, logBaseDir, retentionDurationMs);
      scheduledExecutor.scheduleAtFixedRate(logCleanup, 10,
                                            logCleanupIntervalMins, TimeUnit.MINUTES);
    } catch (Exception e) {
      close();
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    LoggingContext loggingContext = LoggingContextAccessor.getLoggingContext();
    if (loggingContext == null) {
      return;
    }

    try {
      GenericRecord datum = LoggingEvent.encode(logSchema, eventObject);
      logFileWriter.append(ImmutableList.of(new LogWriteEvent(datum, eventObject, loggingContext)));
    } catch (Throwable t) {
      LOG.error("Got exception while serializing log event {}.", eventObject, t);
    }
  }

  private void close() {
    try {
      if (logFileWriter != null) {
        logFileWriter.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }

    super.stop();
    scheduledExecutor.shutdownNow();
    close();
  }
}
