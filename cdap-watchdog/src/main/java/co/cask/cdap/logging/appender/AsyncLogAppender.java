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

package co.cask.cdap.logging.appender;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusManager;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Delegates all calls to an async log appender.
 */
public final class AsyncLogAppender extends LogAppender {
  private final AsyncAppender asyncAppender;
  private final LogAppender logAppender;

  public AsyncLogAppender(LogAppender logAppender) {
    this.logAppender = logAppender;

    this.asyncAppender = new AsyncAppender();
    asyncAppender.setIncludeCallerData(true);
    asyncAppender.setQueueSize(1000);
    asyncAppender.setDiscardingThreshold(0);
    asyncAppender.setName("async-" + logAppender.getName());
  }

  @Override
  public String getName() {
    return "forward-" + asyncAppender.getName();
  }

  @Override
  public void append(LogMessage logMessage) {
    asyncAppender.doAppend(logMessage);
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) {
    append(eventObject);
  }

  @Override
  public void setName(String name) {
    logAppender.setName(name);
    asyncAppender.setName("async-" + logAppender.getName());
  }

  @Override
  public void start() {
    logAppender.start();
    asyncAppender.addAppender(logAppender);
    asyncAppender.start();
  }

  @Override
  public void stop() {
    asyncAppender.stop();
    // No need to stop logAppender here since asyncAppender stops it.
    // However, we will need to wait until logAppender has completely stopped, since the wait time in AsyncAppender
    // may not be sufficient for a clean shutdown.
    long startTime = System.currentTimeMillis();
    while (logAppender.isStarted() &&
      System.currentTimeMillis() - startTime < TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS)) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public boolean isStarted() {
    return logAppender.isStarted() && asyncAppender.isStarted();
  }

  @Override
  public String toString() {
    return logAppender.toString();
  }

  @Override
  public void addFilter(Filter<ILoggingEvent> newFilter) {
    logAppender.addFilter(newFilter);
  }

  @Override
  public void clearAllFilters() {
    logAppender.clearAllFilters();
  }

  @Override
  public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList() {
    return logAppender.getCopyOfAttachedFiltersList();
  }

  @Override
  public FilterReply getFilterChainDecision(ILoggingEvent event) {
    return logAppender.getFilterChainDecision(event);
  }

  @Override
  public void setContext(Context context) {
    logAppender.setContext(context);
    asyncAppender.setContext(context);
  }

  @Override
  public Context getContext() {
    return logAppender.getContext();
  }

  @Override
  public StatusManager getStatusManager() {
    return logAppender.getStatusManager();
  }

  @Override
  protected Object getDeclaredOrigin() {
    return logAppender;
  }

  @Override
  public void addStatus(Status status) {
    logAppender.addStatus(status);
  }

  @Override
  public void addInfo(String msg) {
    logAppender.addInfo(msg);
  }

  @Override
  public void addInfo(String msg, Throwable ex) {
    logAppender.addInfo(msg, ex);
  }

  @Override
  public void addWarn(String msg) {
    logAppender.addWarn(msg);
  }

  @Override
  public void addWarn(String msg, Throwable ex) {
    logAppender.addWarn(msg, ex);
  }

  @Override
  public void addError(String msg) {
    logAppender.addError(msg);
  }

  @Override
  public void addError(String msg, Throwable ex) {
    logAppender.addError(msg, ex);
  }
}
