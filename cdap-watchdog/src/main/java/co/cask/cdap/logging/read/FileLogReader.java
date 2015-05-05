/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.logging.read;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.AndFilter;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Reads log events from a file.
 */
public class FileLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogReader.class);

  private final FileMetaDataManager fileMetaDataManager;
  private final Schema schema;

  @Inject
  public FileLogReader(CConfiguration cConf, FileMetaDataManager fileMetaDataManager) {
    String baseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");

    try {
      this.schema = new LogSchema().getAvroSchema();
      this.fileMetaDataManager = fileMetaDataManager;

    } catch (Exception e) {
      LOG.error("Got exception", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void getLogNext(final LoggingContext loggingContext, final ReadRange readRange, final int maxEvents,
                              final Filter filter, final Callback callback) {
    if (readRange == ReadRange.LATEST) {
      getLogPrev(loggingContext, readRange, maxEvents, filter, callback);
      return;
    }

    callback.init();

    try {
      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));
      long fromTimeMs = readRange.getFromMillis() + 1;

      SortedMap<Long, Location> sortedFiles = fileMetaDataManager.listFiles(loggingContext);
      if (sortedFiles.isEmpty()) {
        return;
      }

      long prevInterval = -1;
      Location prevPath = null;
      List<Location> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
      for (Map.Entry<Long, Location> entry : sortedFiles.entrySet()) {
        if (entry.getKey() >= readRange.getFromMillis() &&
          prevPath != null && entry.getKey() <= readRange.getToMillis()) {
          tailFiles.add(prevPath);
        }
        prevInterval = entry.getKey();
        prevPath = entry.getValue();
      }

      if (prevInterval != -1) {
        tailFiles.add(prevPath);
      }

      AvroFileReader logReader = new AvroFileReader(schema);
      for (Location file : tailFiles) {
        logReader.readLog(file, logFilter, fromTimeMs, Long.MAX_VALUE, maxEvents - callback.getCount(), callback);
        if (callback.getCount() >= maxEvents) {
          break;
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw  Throwables.propagate(e);
    }
  }

  @Override
  public void getLogPrev(final LoggingContext loggingContext, final ReadRange readRange, final int maxEvents,
                              final Filter filter, final Callback callback) {
    callback.init();
    try {
      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));

      SortedMap<Long, Location> sortedFiles =
        ImmutableSortedMap.copyOf(fileMetaDataManager.listFiles(loggingContext), Collections.<Long>reverseOrder());
      if (sortedFiles.isEmpty()) {
        return;
      }

      long fromTimeMs = readRange != ReadRange.LATEST ? readRange.getToMillis() - 1 : System.currentTimeMillis();

      List<Location> tailFiles = Lists.newArrayListWithExpectedSize(sortedFiles.size());
      for (Map.Entry<Long, Location> entry : sortedFiles.entrySet()) {
        if (entry.getKey() >= readRange.getFromMillis() && entry.getKey() <= readRange.getToMillis()) {
          tailFiles.add(entry.getValue());
        }
      }

      List<Collection<LogEvent>> logSegments = Lists.newLinkedList();
      AvroFileReader logReader = new AvroFileReader(schema);
      int count = 0;
      for (Location file : tailFiles) {
        Collection<LogEvent> events = logReader.readLogPrev(file, logFilter, fromTimeMs, maxEvents - count);
        logSegments.add(events);
        count += events.size();
        if (count >= maxEvents) {
          break;
        }
      }

      for (LogEvent event : Iterables.concat(Lists.reverse(logSegments))) {
        callback.handle(event);
      }
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw  Throwables.propagate(e);
    }
  }

  @Override
  public void getLog(final LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                          final Filter filter, final Callback callback) {
    callback.init();
    try {
      Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                        filter));

      SortedMap<Long, Location> sortedFiles = fileMetaDataManager.listFiles(loggingContext);
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

      AvroFileReader avroFileReader = new AvroFileReader(schema);
      for (Location file : files) {
        avroFileReader.readLog(file, logFilter, fromTimeMs, toTimeMs, Integer.MAX_VALUE, callback);
      }
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw  Throwables.propagate(e);
    }
  }
}
