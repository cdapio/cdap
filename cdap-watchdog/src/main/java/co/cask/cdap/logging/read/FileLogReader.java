/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.AndFilter;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

/**
 * Reads log events from a file.
 */
public class FileLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogReader.class);

  private final FileMetaDataManager fileMetaDataManager;
  private final Schema schema;
  private final Impersonator impersonator;

  @Inject
  public FileLogReader(CConfiguration cConf, FileMetaDataManager fileMetaDataManager, Impersonator impersonator) {
    String baseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Preconditions.checkNotNull(baseDir, "Log base dir cannot be null");

    try {
      this.schema = new LogSchema().getAvroSchema();
      this.fileMetaDataManager = fileMetaDataManager;
      this.impersonator = impersonator;
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

      LOG.trace("Using fromTimeMs={}, readRange={}", fromTimeMs, readRange);
      NavigableMap<Long, Location> sortedFiles = fileMetaDataManager.listFiles(loggingContext);
      if (sortedFiles.isEmpty()) {
        return;
      }

      List<Location> filesInRange = getFilesInRange(sortedFiles, readRange.getFromMillis(), readRange.getToMillis());
      AvroFileReader logReader = new AvroFileReader(schema);
      NamespaceId namespaceId = LoggingContextHelper.getNamespaceId(loggingContext);
      for (Location file : filesInRange) {
        try {
          LOG.trace("Reading file {}", file);
          logReader.readLog(file, logFilter, fromTimeMs,
                            Long.MAX_VALUE, maxEvents - callback.getCount(), callback, namespaceId, impersonator);
          if (callback.getCount() >= maxEvents) {
            break;
          }
        } catch (IOException e) {
          LOG.warn("Got exception reading log file {}", file, e);
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

      NavigableMap<Long, Location> sortedFiles = fileMetaDataManager.listFiles(loggingContext);
      if (sortedFiles.isEmpty()) {
        return;
      }

      long fromTimeMs = readRange.getToMillis() - 1;

      LOG.trace("Using fromTimeMs={}, readRange={}", fromTimeMs, readRange);
      List<Location> filesInRange = getFilesInRange(sortedFiles, readRange.getFromMillis(), readRange.getToMillis());
      List<Collection<LogEvent>> logSegments = Lists.newLinkedList();
      AvroFileReader logReader = new AvroFileReader(schema);
      int count = 0;
      NamespaceId namespaceId = LoggingContextHelper.getNamespaceId(loggingContext);
      for (Location file : Lists.reverse(filesInRange)) {
        try {
          LOG.trace("Reading file {}", file);

          Collection<LogEvent> events = logReader.readLogPrev(file, logFilter, fromTimeMs, maxEvents - count,
                                                              namespaceId, impersonator);
          logSegments.add(events);
          count += events.size();
          if (count >= maxEvents) {
            break;
          }
        } catch (IOException e) {
          LOG.warn("Got exception reading log file {}", file, e);
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
  public CloseableIterator<LogEvent> getLog(LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                                            Filter filter) {
    try {
      final Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                              filter));

      LOG.trace("Using fromTimeMs={}, toTimeMs={}", fromTimeMs, toTimeMs);
      NavigableMap<Long, Location> sortedFiles = fileMetaDataManager.listFiles(loggingContext);
      if (sortedFiles.isEmpty()) {
        // return empty iterator
        return new AbstractCloseableIterator<LogEvent>() {
          @Override
          protected LogEvent computeNext() {
            endOfData();
            return null;
          }

          @Override
          public void close() {
            // no-op
          }
        };
      }

      List<Location> filesInRange = getFilesInRange(sortedFiles, fromTimeMs, toTimeMs);

      final AvroFileReader avroFileReader = new AvroFileReader(schema);
      final Iterator<Location> filesIter = filesInRange.iterator();
      final NamespaceId namespaceId = LoggingContextHelper.getNamespaceId(loggingContext);

      CloseableIterator closeableIterator = new CloseableIterator() {
        private CloseableIterator<LogEvent> curr = null;

        @Override
        public void close() {
          if (curr != null) {
            curr.close();
          }
        }

        @Override
        public boolean hasNext() {
          return filesIter.hasNext();
        }

        @Override
        public CloseableIterator<LogEvent> next() {
          if (curr != null) {
            curr.close();
          }
          Location file = filesIter.next();
          LOG.trace("Reading file {}", file);
          curr = avroFileReader.readLog(file, logFilter, fromTimeMs, toTimeMs, Integer.MAX_VALUE,
                                        namespaceId, impersonator);
          return curr;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove not supported");
        }
      };

      return concat(closeableIterator);
    } catch (Throwable e) {
      LOG.error("Got exception: ", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * See {@link com.google.common.collect.Iterators#concat(Iterator)}. The difference is that the input types and return
   * type are CloseableIterator, which closes the inputs that it has opened.
   */
  public static <T> CloseableIterator<T> concat(
    final CloseableIterator<? extends CloseableIterator<? extends T>> inputs) {
    Preconditions.checkNotNull(inputs);
    return new CloseableIterator<T>() {
      @Override
      public void close() {
        current.close();
        current = null;
        while (inputs.hasNext()) {
          inputs.next().close();
        }
        inputs.close();
        removeFrom = null;
      }

      CloseableIterator<? extends T> current = new CloseableIterator<T>() {
        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public T next() {
          throw new NoSuchElementException();
        }

        @Override
        public void remove() {
          throw new IllegalStateException();
        }
      };

      CloseableIterator<? extends T> removeFrom;

      @Override
      public boolean hasNext() {
        // http://code.google.com/p/google-collections/issues/detail?id=151
        // current.hasNext() might be relatively expensive, worth minimizing.
        boolean currentHasNext;
        // checkNotNull eager for GWT
        // note: it must be here & not where 'current' is assigned,
        // because otherwise we'll have called inputs.next() before throwing
        // the first NPE, and the next time around we'll call inputs.next()
        // again, incorrectly moving beyond the error.
        while (!(currentHasNext = Preconditions.checkNotNull(current).hasNext())
          && inputs.hasNext()) {
          current = inputs.next();
        }
        return currentHasNext;
      }
      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        removeFrom = current;
        return current.next();
      }
      @Override
      public void remove() {
        Preconditions.checkState(removeFrom != null,
                                 "no calls to next() since last call to remove()");
        removeFrom.remove();
        removeFrom = null;
      }
    };
  }

  @VisibleForTesting
  static List<Location> getFilesInRange(NavigableMap<Long, Location> sortedFiles, long fromTimeMs, long toTimeMs) {
    // Get a list of files to read based on fromMillis and toMillis.
    // Each file is associated with the time of the first log message in it.
    // Let c be the file with the largest timestamp smaller than readRange.getFromMillis().
    // We need to select all the files within the range [c, readRange.toMillis()).
    Long start = sortedFiles.floorKey(fromTimeMs);
    if (start == null) {
      start = sortedFiles.firstKey();

      // It is possible that the log files for the requested range are already
      // deleted, in case of old program runs. For such requests both the start and toTimeMs
      // will fall outside the range sortedFiles. In that case return empty list.
      if (start > toTimeMs) {
        return ImmutableList.of();
      }
    }
    return ImmutableList.copyOf(sortedFiles.subMap(start, toTimeMs).values());
  }
}
