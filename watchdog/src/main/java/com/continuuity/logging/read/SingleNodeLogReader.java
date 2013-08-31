/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.context.LoggingContextHelper;
import com.continuuity.logging.filter.AndFilter;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads log events in single node setup.
 */
public class SingleNodeLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(SingleNodeLogReader.class);

  private static final int MAX_THREAD_POOL_SIZE = 20;

  private final Location logBaseDir;
  private final Schema schema;
  private final ExecutorService executor;

  @Inject
  public SingleNodeLogReader(CConfiguration cConf, LocationFactory locationFactory) {
    String baseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");
    this.logBaseDir = locationFactory.create(baseDir);

    try {
      this.schema = new LogSchema().getAvroSchema();
    } catch (IOException e) {
      LOG.error("Cannot get LogSchema", e);
      throw Throwables.propagate(e);
    }

    // Thread pool of size max MAX_THREAD_POOL_SIZE.
    // 60 seconds wait time before killing idle threads.
    // Keep no idle threads more than 60 seconds.
    // If max thread pool size reached, reject the new coming
    this.executor =
      new ThreadPoolExecutor(0, MAX_THREAD_POOL_SIZE,
                             60L, TimeUnit.SECONDS,
                             new SynchronousQueue<Runnable>(),
                             Threads.createDaemonThreadFactory("single-log-reader-%d"),
                             new ThreadPoolExecutor.DiscardPolicy());
  }

  @Override
  public void getLogNext(final LoggingContext loggingContext, final long fromOffset, final int maxEvents,
                              final Filter filter, final Callback callback) {
    if (fromOffset < 0) {
      getLogPrev(loggingContext, -1, maxEvents, filter, callback);
      return;
    }

    executor.submit(
      new Runnable() {
        @Override
        public void run() {
          callback.init();

          try {
            Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                              filter));
            long fromTimeMs = fromOffset + 1;

            SortedMap<Long, Location> sortedFiles = getFiles(null);
            if (sortedFiles.isEmpty()) {
              return;
            }

            long prevInterval = -1;
            Location prevPath = null;
            List<Location> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
            for (Map.Entry<Long, Location> entry : sortedFiles.entrySet()) {
              if (entry.getKey() >= fromTimeMs && prevPath != null) {
                tailFiles.add(prevPath);
              }
              prevInterval = entry.getKey();
              prevPath = entry.getValue();
            }

            if (prevInterval != -1) {
              tailFiles.add(prevPath);
            }

            AvroFileLogReader logReader = new AvroFileLogReader(schema);
            CountingCallback countingCallback = new CountingCallback(callback);
            for (Location file : tailFiles) {
              logReader.readLog(file, logFilter, fromTimeMs, Long.MAX_VALUE, maxEvents - countingCallback.getCount(),
                                countingCallback);
              if (countingCallback.getCount() >= maxEvents) {
                break;
              }
            }
          } finally {
            callback.close();
          }
        }
      }
    );
  }

  /**
   * Counts the number of times handle is called.
   */
  private static class CountingCallback implements Callback {
    private final Callback callback;
    private final AtomicInteger count = new AtomicInteger(0);

    private CountingCallback(Callback callback) {
      this.callback = callback;
    }

    @Override
    public void init() {
    }

    @Override
    public void handle(LogEvent event) {
      count.incrementAndGet();
      callback.handle(event);
    }

    public int getCount() {
      return count.get();
    }

    @Override
    public void close() {
    }
  }

  @Override
  public void getLogPrev(final LoggingContext loggingContext, final long fromOffset, final int maxEvents,
                              final Filter filter, final Callback callback) {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        callback.init();
        try {
          Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                            filter));

          SortedMap<Long, Location> sortedFiles = getFiles(Collections.<Long>reverseOrder());
          if (sortedFiles.isEmpty()) {
            return;
          }

          long fromTimeMs = fromOffset >= 0 ? fromOffset - 1 : System.currentTimeMillis();

          List<Location> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
          for (Map.Entry<Long, Location> entry : sortedFiles.entrySet()) {
            if (entry.getKey() <= fromTimeMs) {
              tailFiles.add(entry.getValue());
            }
          }

          List<ILoggingEvent> loggingEvents = Lists.newLinkedList();
          AvroFileLogReader logReader = new AvroFileLogReader(schema);
          for (Location file : tailFiles) {
            Collection<ILoggingEvent> events = logReader.readLogPrev(file, logFilter, fromTimeMs,
                                                                     maxEvents - loggingEvents.size());
            loggingEvents.addAll(0, events);
            if (events.size() >= maxEvents) {
              break;
            }
          }

          // TODO: better algorithm to read previous events
          for (ILoggingEvent event : loggingEvents) {
            callback.handle(new LogEvent(event, event.getTimeStamp()));
          }
        } finally {
          callback.close();
        }
      }
    });
  }

  @Override
  public void getLog(final LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                          final Filter filter, final Callback callback) {
    executor.submit(
      new Runnable() {
        @Override
        public void run() {
          callback.init();
          try {
            Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                              filter));

            SortedMap<Long, Location> sortedFiles = getFiles(null);
            if (sortedFiles.isEmpty()) {
              return;
            }

            long prevInterval = -1;
            Location prevPath = null;
            List<Location> files = Lists.newArrayListWithExpectedSize(sortedFiles.size());
            for (Map.Entry<Long, Location> entry : sortedFiles.entrySet()) {
              if (entry.getKey() >= fromTimeMs && prevInterval != -1 && prevInterval < toTimeMs) {
                files.add(prevPath);
              }
              prevInterval = entry.getKey();
              prevPath = entry.getValue();
            }

            if (prevInterval != -1 && prevInterval < toTimeMs) {
              files.add(prevPath);
            }

            AvroFileLogReader avroFileLogReader = new AvroFileLogReader(schema);
            for (Location file : files) {
              avroFileLogReader.readLog(file, logFilter, fromTimeMs, toTimeMs, Integer.MAX_VALUE, callback);
            }
          } finally {
            callback.close();
          }
        }
      }
    );
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private SortedMap<Long, Location> getFiles(Comparator<Long> comparator) {
    TreeMap<Long, Location> sortedFiles = Maps.newTreeMap(comparator);
    File baseDir = new File(logBaseDir.toURI());
    File [] files = baseDir.listFiles();
    if (files == null || files.length == 0) {
      return sortedFiles;
    }

    for (File file : files){
      try {
        long interval = extractInterval(file.getName());
        sortedFiles.put(interval, new SeekableLocalLocation(logBaseDir.append(file.getName())));
      } catch (NumberFormatException e) {
        LOG.warn(String.format("Not able to parse interval from log file name %s", file.getPath()));
      } catch (IOException e) {
        LOG.warn("Got exception", e);
      }
    }
    return sortedFiles;
  }

  private static long extractInterval(String fileName) {
    // File name format is timestamp.avro
    if (fileName == null || fileName.isEmpty()) {
      return -1;
    }
    int endIndex = fileName.indexOf('.');
    return Long.parseLong(fileName.substring(0, endIndex));
  }
}
