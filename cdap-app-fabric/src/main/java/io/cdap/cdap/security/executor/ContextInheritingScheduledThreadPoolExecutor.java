/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.security.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A {@link java.util.concurrent.ScheduledThreadPoolExecutor} which inherits
 * {@link io.cdap.cdap.security.spi.authentication.SecurityRequestContext}. This is intended to ensure any call to the
 * pool has the correct security context from the parent.
 *
 * Note that any downstream thread pool executor must use this class as well, as any calls to a thread pool which does
 * not inherit context will cause other threads to inherit the wrong context.
 */
public class ContextInheritingScheduledThreadPoolExecutor implements ScheduledExecutorService {

  private final ScheduledThreadPoolExecutor delegate;

  public ContextInheritingScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
    this.delegate = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);
  }

  @Override
  public void execute(Runnable command) {
    delegate.execute(ContextExecutors.wrapRunnableWithContext(command));
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(ContextExecutors.wrapCallableWithContext(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(ContextExecutors.wrapRunnableWithContext(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(ContextExecutors.wrapRunnableWithContext(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return delegate.invokeAll(tasks.stream().map(c -> ContextExecutors.wrapCallableWithContext(c))
                                .collect(Collectors.toList()));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException {
    return delegate.invokeAll(tasks.stream().map(c -> ContextExecutors.wrapCallableWithContext(c))
                                .collect(Collectors.toList()), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return delegate.invokeAny(tasks.stream().map(c -> ContextExecutors.wrapCallableWithContext(c))
                                .collect(Collectors.toList()));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.invokeAny(tasks.stream().map(c -> ContextExecutors.wrapCallableWithContext(c))
                                .collect(Collectors.toList()), timeout, unit);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return delegate.schedule(ContextExecutors.wrapRunnableWithContext(command), delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return delegate.schedule(ContextExecutors.wrapCallableWithContext(callable), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return delegate.scheduleAtFixedRate(ContextExecutors.wrapRunnableWithContext(command), initialDelay,
                                        period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return delegate.scheduleWithFixedDelay(ContextExecutors.wrapRunnableWithContext(command), initialDelay,
                                           delay, unit);
  }
}
