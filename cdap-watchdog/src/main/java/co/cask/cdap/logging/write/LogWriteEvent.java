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

package co.cask.cdap.logging.write;

import co.cask.cdap.api.log.LoggingEvent;
import co.cask.cdap.common.logging.LoggingContext;
import com.google.common.primitives.Longs;
import org.apache.avro.generic.GenericRecord;

/**
 * Represents a log event that can be written to avro file.
 */
public class LogWriteEvent implements Comparable<LogWriteEvent> {

  protected final GenericRecord  genericRecord;
  protected final LoggingEvent logEvent;
  protected final LoggingContext loggingContext;

  public LogWriteEvent(GenericRecord genericRecord, LoggingEvent logEvent, LoggingContext loggingContext) {
    this.genericRecord = genericRecord;
    this.logEvent = logEvent;
    this.loggingContext = loggingContext;
  }

  public GenericRecord getGenericRecord() {
    return genericRecord;
  }

  public LoggingEvent getLogEvent() {
    return logEvent;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Override
  public int compareTo(LogWriteEvent event) {
    return Longs.compare(logEvent.getTimeStamp(), event.getLogEvent().getTimeStamp());
  }
}
