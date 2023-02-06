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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link ThreadPoolExecutor} which inherits {@link io.cdap.cdap.security.spi.authentication.SecurityRequestContext}.
 */
public class ContextInheritingThreadPoolExecutor implements ExecutorService {

  private final ThreadPoolExecutor delegate;

  public ContextInheritingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                             TimeUnit unit, BlockingQueue<Runnable> workQueue,
                                             ThreadFactory threadFactory) {
    this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, null);
  }

  public ContextInheritingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                             TimeUnit unit, BlockingQueue<Runnable> workQueue,
                                             ThreadFactory threadFactory, @Nullable Runnable terminationHook) {
    this.delegate = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime,
                                           unit, workQueue, threadFactory) {
      @Override
      protected void terminated() {
        if (terminationHook != null) {
          terminationHook.run();
        }
      }
    };
  }

  public void allowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
    this.delegate.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
  }


  @Override
  public void execute(Runnable command) {
    delegate.execute(SecurityContextRunnables.wrapRunnableWithContext(command));
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
    return delegate.submit(SecurityContextRunnables.wrapCallableWithContext(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(SecurityContextRunnables.wrapRunnableWithContext(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(SecurityContextRunnables.wrapRunnableWithContext(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return delegate.invokeAll(tasks.stream().map(c -> SecurityContextRunnables.wrapCallableWithContext(c))
                                .collect(Collectors.toList()));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException {
    return delegate.invokeAll(tasks.stream().map(c -> SecurityContextRunnables.wrapCallableWithContext(c))
                                .collect(Collectors.toList()), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return delegate.invokeAny(tasks.stream().map(c -> SecurityContextRunnables.wrapCallableWithContext(c))
                                .collect(Collectors.toList()));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.invokeAny(tasks.stream().map(c -> SecurityContextRunnables.wrapCallableWithContext(c))
                                .collect(Collectors.toList()), timeout, unit);
  }
}
