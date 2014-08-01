/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Represents a log event returned by LogReader.
 */
public class LogEvent {
  private final ILoggingEvent loggingEvent;
  private final long offset;

  public LogEvent(ILoggingEvent loggingEvent, long offset) {
    this.loggingEvent = loggingEvent;
    this.offset = offset;
  }

  public ILoggingEvent getLoggingEvent() {
    return loggingEvent;
  }

  public long getOffset() {
    return offset;
  }
}
