/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link FileReader} that provides continuous event stream that never end.
 *
 * @param <T> Type of event.
 * @param <P> Type of position object.
 */
@NotThreadSafe
public abstract class LiveFileReader<T, P> implements FileReader<T, P> {

  private FileReader<T, P> currentReader;
  private FileReader<T, P> nextReader;

  @Override
  public void initialize() throws IOException {
    if (currentReader == null) {
      currentReader = renewReader();
    }
  }

  @Override
  public int read(Collection<? super T> events, int maxEvents,
                  long timeout, TimeUnit unit) throws IOException, InterruptedException {
    return read(events, maxEvents, timeout, unit, ReadFilter.ALWAYS_ACCEPT);
  }

  @Override
  public int read(Collection<? super T> events, int maxEvents,
                  long timeout, TimeUnit unit, ReadFilter readFilter) throws IOException, InterruptedException {
    if (currentReader == null) {
      currentReader = renewReader();
    }
    // No data yet
    if (currentReader == null) {
      return 0;
    }

    long startTime = System.nanoTime();
    int eventCount = currentReader.read(events, maxEvents, timeout, unit, readFilter);
    long timeElapse = System.nanoTime() - startTime;

    if (eventCount > 0) {
      return eventCount;
    }

    if (nextReader == null) {
      nextReader = renewReader();
    }

    if (nextReader != null) {
      long readTimeout = unit.toNanos(timeout) - timeElapse;
      if (readTimeout < 0) {
        readTimeout = 0;
      }

      if (eventCount == 0) {
        // Not yet EOF. Since next reader is already available, it could either be the reader doesn't see
        // the last flush from the writer in the read() above or the writer actually crashed.
        // To handle these cases, an extra read is done when a new reader is available but current read
        // gives no event, so that
        // 1. If the writer properly closed the file, by the time we see a new file here, an extra read should be
        //    able to see events till the end of file, as writer won't create new file before old one is closed.
        // 2. If the writer crashed, an extra read will still yield no event, but that's ok, as no more write will
        //    be happening to the old file.
        eventCount = currentReader.read(events, maxEvents, readTimeout, TimeUnit.NANOSECONDS, readFilter);
      }

      if (eventCount <= 0) {
        // Only switch reader when nothing get read above as it guaranteed no more events can come from the
        // currentReader since new file is already available.
        Closeables.closeQuietly(currentReader);
        currentReader = nextReader;
        nextReader = null;

        // If it's EOF, ok to read from new reader one more time without adjusting the readTime because a EOF
        // read (happened above) should return immediately.
        // Otherwise, if no events from current reader, it's safe to read from the new one, but with 0 timeout,
        // assuming the read happened above used up all readTimeout allowed time.
        if (eventCount < 0) {
          eventCount = currentReader.read(events, maxEvents, readTimeout, TimeUnit.NANOSECONDS, readFilter);
        } else {
          eventCount = currentReader.read(events, maxEvents, 0, TimeUnit.NANOSECONDS, readFilter);
        }
      }
    }

    // It never reach EOF
    return (eventCount < 0) ? 0 : eventCount;
  }

  @Override
  public void close() throws IOException {
    try {
      if (currentReader != null) {
        currentReader.close();
      }
    } finally {
      if (nextReader != null) {
        nextReader.close();
      }
    }
  }

  @Override
  public P getPosition() {
    Preconditions.checkState(currentReader != null, "Reader position unknown.");
    return currentReader.getPosition();
  }

  /**
   * @return A new FileReader or {@code null} if no update.
   */
  @Nullable
  protected abstract FileReader<T, P> renewReader() throws IOException;
}
