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

package co.cask.cdap.logging.appender;

import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;

import java.io.Flushable;
import java.io.IOException;

/**
 * An {@link Appender} for appending events by delegating to another
 * {@link Appender}. It also implements {@link Flushable} interface by optionally forwarding to another
 * {@link Appender} if that implemented {@link Flushable} as well.
 *
 * If the call to {@link Flushable#flush()} failed, flushing will be retried on the next
 * {@link Appender#doAppend(Object)} call before appending more events. This is the mechanism to prevent
 * underlying appender implementation buffered too many events if flushing is consistently failing, causing
 * out of memory of the pipeline processor.
 *
 * @param <E> type of events
 */
public final class FlushableAppender<E> extends ForwardingAppender<E> implements Flushable {

  private boolean needFlush;

  public FlushableAppender(Appender<E> delegate) {
    super(delegate);
  }

  @Override
  public void doAppend(E event) throws LogbackException {
    if (needFlush) {
      try {
        flush();
      } catch (IOException e) {
        throw new LogbackException("Flush failed. Cannot append event.", e);
      }
    }
    super.doAppend(event);
  }

  @Override
  public void flush() throws IOException {
    Appender<E> appender = getDelegate();
    if (appender instanceof Flushable) {
      try {
        ((Flushable) appender).flush();
        needFlush = false;
      } catch (Exception e) {
        needFlush = true;
        throw e;
      }
    }
  }
}
