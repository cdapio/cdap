/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline.logbuffer;

import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.Objects;

/**
 * Log event with file offset.
 */
public final class LogBufferEvent {
  private final ILoggingEvent logEvent;
  private final FileOffset offset;

  public LogBufferEvent(ILoggingEvent logEvent, FileOffset offset) {
    this.logEvent = logEvent;
    this.offset = offset;
  }

  public ILoggingEvent getLogEvent() {
    return logEvent;
  }

  public FileOffset getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogBufferEvent that = (LogBufferEvent) o;
    return Objects.equals(logEvent, that.logEvent) && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(logEvent, offset);
  }

  @Override
  public String toString() {
    return "BufferLogEvent{" +
      "logEvent=" + logEvent +
      ", offset=" + offset +
      '}';
  }
}
