/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.spi.logs;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Publishes log events to a destination.
 */
public interface LogPublisher extends AutoCloseable {

  /**
   * Returns the name of the log publisher.
   *
   * @return the name of the publisher.
   */
  String getName();

  /**
   * Publishes a logging event.
   *
   * @param event the logging event to publish.
   */
  void publish(ILoggingEvent event);

  /**
   * Initializes the log publisher with the required context.
   *
   * @param context the context to initialize the publisher with.
   */
  void initialize(LogPublisherContext context);

  /**
   * Closes the log publisher and releases associated resources.
   */
  @Override
  void close();
}
