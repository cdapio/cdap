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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.common.lang.InstantiatorFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.reflect.TypeToken;
import org.apache.twill.common.Cancellable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An abstract base implementation of {@link DelegatorContext} to provide context per thread implementation,
 * together with the context capturing capability that is suitable for {@link HttpContentProducer} and
 * {@link HttpContentConsumer} use cases.
 *
 * @param <T> type of the user service handler
 */
public abstract class AbstractDelegatorContext<T> implements DelegatorContext<T>, Closeable {

  private final TypeToken<T> handlerType;
  private final InstantiatorFactory instantiatorFactory;
  private final LoadingCache<Thread, HandlerTaskExecutor> handlerExecutorCache;
  private final Queue<HandlerTaskExecutor> handlerExecutorPool;
  private final AtomicInteger handlerExecutorSize;
  private final MetricsContext programMetricsContext;
  private final MetricsContext handlerMetricsContext;
  private volatile boolean shutdown;


  protected AbstractDelegatorContext(TypeToken<T> handlerType, InstantiatorFactory instantiatorFactory,
                                     MetricsContext programMetricsContext, MetricsContext handlerMetricsContext) {
    this.handlerType = handlerType;
    this.instantiatorFactory = instantiatorFactory;
    this.programMetricsContext = programMetricsContext;
    this.handlerMetricsContext = handlerMetricsContext;
    this.handlerExecutorPool = new ConcurrentLinkedQueue<>();
    this.handlerExecutorCache = createHandlerTaskExecutorCache();
    this.handlerExecutorSize = new AtomicInteger();
  }

  /**
   * Returns the {@link MetricsContext} for the user service handler.
   */
  public MetricsContext getHandlerMetricsContext() {
    return handlerMetricsContext;
  }

  @Override
  public final T getHandler() {
    return handlerExecutorCache.getUnchecked(Thread.currentThread()).getHandler();
  }

  @Override
  public final ServiceTaskExecutor getServiceTaskExecutor() {
    return handlerExecutorCache.getUnchecked(Thread.currentThread());
  }

  @Override
  public final Cancellable capture() {
    // To capture, remove the executor from the cache.
    // The removal listener of the cache will be triggered for this thread entry with an EXPLICIT cause
    final HandlerTaskExecutor executor = handlerExecutorCache.asMap().remove(Thread.currentThread());
    if (executor == null) {
      // Shouldn't happen, as the executor should of the current thread must be in the cache
      // Otherwise, it's a bug in the system.
      throw new IllegalStateException("Handler context not found for thread " + Thread.currentThread());
    }

    final AtomicBoolean cancelled = new AtomicBoolean(false);
    return () -> {
      if (cancelled.compareAndSet(false, true)) {
        handlerExecutorPool.offer(executor);
        // offer never return false for ConcurrentLinkedQueue
        programMetricsContext.gauge("context.pool.size", handlerExecutorSize.incrementAndGet());
      } else {
        // This shouldn't happen, unless there is bug in the platform.
        // Since the context capture and release is a complicated logic, it's better throwing exception
        // to guard against potential future bug.
        throw new IllegalStateException("Captured context cannot be released twice.");
      }
    };
  }

  /**
   * Cleanup user service handler instances that are not longer in use.
   */
  public final void cleanUp() {
    // Invalid all cached entries if the corresponding thread is no longer running
    List<Thread> invalidKeys = new ArrayList<>();
    for (Map.Entry<Thread, HandlerTaskExecutor> entry : handlerExecutorCache.asMap().entrySet()) {
      if (!entry.getKey().isAlive()) {
        invalidKeys.add(entry.getKey());
      }
    }
    handlerExecutorCache.invalidateAll(invalidKeys);
    handlerExecutorCache.cleanUp();
  }

  @Override
  public final void close() {
    shutdown = true;
    handlerExecutorCache.invalidateAll();
    handlerExecutorCache.cleanUp();
    handlerExecutorPool.forEach(HandlerTaskExecutor::close);
    handlerExecutorPool.clear();
  }

  /**
   * Returns the type of the user service handler.
   */
  public final TypeToken<T> getHandlerType() {
    return handlerType;
  }

  /**
   * Creates an instance of {@link HandlerTaskExecutor} with a new user service handler instance.
   *
   * @param instantiatorFactory the {@link InstantiatorFactory} for creating new user service handler instance
   */
  protected abstract HandlerTaskExecutor createTaskExecutor(InstantiatorFactory instantiatorFactory) throws Exception;

  private LoadingCache<Thread, HandlerTaskExecutor> createHandlerTaskExecutorCache() {
    return CacheBuilder.newBuilder()
      .weakKeys()
      .removalListener((RemovalListener<Thread, HandlerTaskExecutor>) notification -> {
        Thread thread = notification.getKey();
        HandlerTaskExecutor executor = notification.getValue();
        if (executor == null) {
          return;
        }
        // If the removal is due to eviction (expired or GC'ed) or
        // if the thread is no longer active, close the associated context.
        if (shutdown || notification.wasEvicted() || thread == null || !thread.isAlive()) {
          executor.close();
        }
      })
      .build(new CacheLoader<Thread, HandlerTaskExecutor>() {
        @Override
        public HandlerTaskExecutor load(Thread key) throws Exception {
          HandlerTaskExecutor executor = handlerExecutorPool.poll();
          if (executor == null) {
            return createTaskExecutor(instantiatorFactory);
          }
          programMetricsContext.gauge("context.pool.size", handlerExecutorSize.decrementAndGet());
          return executor;
        }
      });
  }


  /**
   * Helper class for performing user service handler lifecycle calls as well as task execution.
   */
  public abstract class HandlerTaskExecutor implements ServiceTaskExecutor, Closeable {

    private final T handler;

    protected HandlerTaskExecutor(T handler) throws Exception {
      initHandler(handler);
      this.handler = handler;
    }

    @Override
    public void close() {
      destroyHandler(handler);
    }

    protected T getHandler() {
      return handler;
    }

    protected abstract void initHandler(T handler) throws Exception;

    protected abstract void destroyHandler(T handler);
  }
}
