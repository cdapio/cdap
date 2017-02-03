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
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;

import java.util.List;

/**
 * An {@link Appender} that forwards all methods to another appender.
 *
 * @param <E> type of event that can be appended to this {@link Appender}.
 */
public abstract class ForwardingAppender<E> implements Appender<E> {

  private final Appender<E> delegate;

  protected ForwardingAppender(Appender<E> delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public void doAppend(E event) throws LogbackException {
    delegate.doAppend(event);
  }

  @Override
  public void setName(String name) {
    delegate.setName(name);
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public boolean isStarted() {
    return delegate.isStarted();
  }

  @Override
  public void setContext(Context context) {
    delegate.setContext(context);
  }

  @Override
  public Context getContext() {
    return delegate.getContext();
  }

  @Override
  public void addStatus(Status status) {
    delegate.addStatus(status);
  }

  @Override
  public void addInfo(String msg) {
    delegate.addInfo(msg);
  }

  @Override
  public void addInfo(String msg, Throwable ex) {
    delegate.addInfo(msg, ex);
  }

  @Override
  public void addWarn(String msg) {
    delegate.addWarn(msg);
  }

  @Override
  public void addWarn(String msg, Throwable ex) {
    delegate.addWarn(msg, ex);
  }

  @Override
  public void addError(String msg) {
    delegate.addError(msg);
  }

  @Override
  public void addError(String msg, Throwable ex) {
    delegate.addError(msg, ex);
  }

  @Override
  public void addFilter(Filter<E> newFilter) {
    delegate.addFilter(newFilter);
  }

  @Override
  public void clearAllFilters() {
    delegate.clearAllFilters();
  }

  @Override
  public List<Filter<E>> getCopyOfAttachedFiltersList() {
    return delegate.getCopyOfAttachedFiltersList();
  }

  @Override
  public FilterReply getFilterChainDecision(E event) {
    return delegate.getFilterChainDecision(event);
  }

  protected Appender<E> getDelegate() {
    return delegate;
  }
}
