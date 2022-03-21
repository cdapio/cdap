/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.read;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.filter.AndFilter;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.meta.FileMetaDataReader;
import io.cdap.cdap.logging.write.LogLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads log events from a file.
 */
public class FileLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(FileLogReader.class);

  private final FileMetaDataReader fileMetadataReader;

  @Inject
  public FileLogReader(FileMetaDataReader fileMetadataReader) {
    this.fileMetadataReader = fileMetadataReader;
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
      List<LogLocation> sortedFilesInRange =
        fileMetadataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(loggingContext),
                                     readRange.getFromMillis(), readRange.getToMillis());
      if (sortedFilesInRange.isEmpty()) {
        return;
      }

      for (LogLocation file : sortedFilesInRange) {
        LOG.trace("Reading file {}", file);
        file.readLog(logFilter, fromTimeMs, Long.MAX_VALUE, maxEvents - callback.getCount(), callback);
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


      List<LogLocation> sortedFilesInRange =
        fileMetadataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(loggingContext),
                                     readRange.getFromMillis(), readRange.getToMillis());
      if (sortedFilesInRange.isEmpty()) {
        return;
      }

      long fromTimeMs = readRange.getToMillis() - 1;

      LOG.trace("Using fromTimeMs={}, readRange={}", fromTimeMs, readRange);
      List<Collection<LogEvent>> logSegments = Lists.newLinkedList();
      int count = 0;
      for (LogLocation file : Lists.reverse(sortedFilesInRange)) {
        try {
          LOG.trace("Reading file {}", file);

          Collection<LogEvent> events = file.readLogPrev(logFilter, fromTimeMs, maxEvents - count);
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
      List<LogLocation> sortedFilesInRange =
        fileMetadataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(loggingContext), fromTimeMs, toTimeMs);

      if (sortedFilesInRange.isEmpty()) {
        // return empty iterator
        return new AbstractCloseableIterator<LogEvent>() {
          @Override
          protected LogEvent computeNext() {
            return endOfData();
          }

          @Override
          public void close() {
            // no-op
          }
        };
      }

      final Iterator<LogLocation> filesIter = sortedFilesInRange.iterator();

      CloseableIterator<CloseableIterator<LogEvent>> closeableIterator =
        new CloseableIterator<CloseableIterator<LogEvent>>() {
          private CloseableIterator<LogEvent> curr;

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
            LogLocation file = filesIter.next();
            LOG.trace("Reading file {}", file);
            curr = file.readLog(logFilter, fromTimeMs, toTimeMs, Integer.MAX_VALUE);
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
}
