/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.api.log;

import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.Iterator;
import java.util.Properties;

/**
 * LogProcessor to receive log messages from CDAP log.saver
 */
public interface LogProcessor {

  /**
   * Called during initialize, passed properties for log processor
   * @param properties
   */
  void initialize(Properties properties);

  /**
   * Process method will be called with iterator of log messages, log messages received will be in sorted order,
   * sorted by timestamp. This method should not throw any exception, if any unchecked exceptions are thrown,
   * log.saver will log an error and the processor will not receive messages.
   * Will start receiving messages on log.saver startup
   *
   * @param events list of {@link ILoggingEvent}
   */
  void process(Iterator<ILoggingEvent> events);

  /**
   * stop logprocessor
   */
  void stop();
}
