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
package co.cask.cdap.data.file;

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

    if (eventCount > 0) {
      return eventCount;
    }

    long timeoutNano = unit.toNanos(timeout);
    // Keep reading until either is some data or no more file reader it can try.
    while (eventCount <= 0) {
      if (nextReader == null) {
        nextReader = renewReader();
        if (nextReader == null) {
          break;
        }
      }

      if (eventCount == 0) {
        // Not yet EOF. Since the next reader is already available, it could either be the reader doesn't see
        // the last flush from the writer in the read() above or the writer actually crashed.
        // To handle these cases, an extra read is done when a new reader is available but the current read
        // gives no event, so that:
        // 1. If the writer properly closed the file, by the time we see a new file here, an extra read should be
        //    able to see events up to the end of file, as the writer won't create a new file before the old one is
        //    closed.
        // 2. If the writer crashed, an extra read will still yield no event, but that's ok, as no more writes will
        //    be happening to the old file.
        eventCount = currentReader.read(events, maxEvents, getReadTimeoutNano(startTime, timeoutNano),
                                        TimeUnit.NANOSECONDS, readFilter);
      }

      if (eventCount <= 0) {
        // Only switch reader when nothing get read above as it guaranteed no more events can come from the
        // currentReader since new file is already available.
        Closeables.closeQuietly(currentReader);
        currentReader = nextReader;
        nextReader = null;

        // Try to read events from the new reader.
        eventCount = currentReader.read(events, maxEvents, getReadTimeoutNano(startTime, timeoutNano),
                                        TimeUnit.NANOSECONDS, readFilter);
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

  private long getReadTimeoutNano(long startTime, long timeoutNano) {
    long timeElapse = System.nanoTime() - startTime;
    long readTimeout = timeoutNano - timeElapse;
    return readTimeout < 0 ? 0L : readTimeout;
  }
}
