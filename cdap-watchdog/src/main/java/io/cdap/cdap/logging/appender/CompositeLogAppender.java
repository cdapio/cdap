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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.core.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * A log appender that delegates logging events to a list of other log appenders.
 */
public class CompositeLogAppender extends LogAppender {

  private final List<LogAppender> appenders;

  /**
   * Constructs a CompositeLogAppender with the given list of appenders.
   *
   * @param appenders the list of {@link LogAppender} instances to delegate logging events to
   */
  public CompositeLogAppender(List<LogAppender> appenders) {
    if (appenders == null) {
      throw new IllegalArgumentException("Appenders list cannot be null");
    }

    // Make the appenders list unmodifiable to ensure thread safety and prevent external
    // modifications.
    this.appenders = Collections.unmodifiableList(new ArrayList<>(appenders));
    setName(getClass().getName());
  }

  @Override
  public void start() {
    executeOnAppenders("start", LogAppender::start);
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    executeOnAppenders("stop", LogAppender::stop);
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    executeOnAppenders("appendEvent", appender -> appender.appendEvent(logMessage));
  }

  @Override
  public void setContext(Context context) {
    executeOnAppenders("setContext", appender -> appender.setContext(context));
    super.setContext(context);
  }

  /**
   * Executes a specified action on all appenders and handles any exceptions.
   *
   * @param actionName the name of the action being performed (for logging purposes)
   * @param action     the action to execute on each appender
   * @throws RuntimeException if one or more appenders fail to perform the action
   */
  private void executeOnAppenders(String actionName, Consumer<LogAppender> action) {
    RuntimeException exceptions = null;
    for (LogAppender appender : appenders) {
      try {
        action.accept(appender);
      } catch (Exception e) {
        if (exceptions == null) {
          exceptions = new RuntimeException("One or more appenders failed to " + actionName);
        }
        exceptions.addSuppressed(e);
      }
    }

    if (exceptions != null) {
      throw exceptions;
    }
  }
}
