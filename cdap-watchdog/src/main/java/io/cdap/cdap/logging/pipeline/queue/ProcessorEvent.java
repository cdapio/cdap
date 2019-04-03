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

package co.cask.cdap.logging.pipeline.queue;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Processor event containing event, eventSize and offset.
 * @param <Offset> type of the offset
 */
public class ProcessorEvent<Offset> {
  private final ILoggingEvent event;
  private final int eventSize;
  private final Offset offset;

  /**
   * Processor event containing log event, eventSize and offset
   */
  public ProcessorEvent(ILoggingEvent event, int eventSize, Offset offset) {
    this.event = event;
    this.eventSize = eventSize;
    this.offset = offset;
  }

  /**
   * Returns log event.
   */
  public ILoggingEvent getEvent() {
    return event;
  }

  /**
   * Returns log event size.
   */
  public int getEventSize() {
    return eventSize;
  }

  /**
   * Returns offset.
   */
  public Offset getOffset() {
    return offset;
  }
}
