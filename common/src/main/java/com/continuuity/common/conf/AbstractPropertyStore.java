/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Base implementation of {@link PropertyStore}.
 *
 * @param <T> Type of property
 */
public abstract class AbstractPropertyStore<T> implements PropertyStore<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractPropertyStore.class);
  private final Multimap<String, ListenerCaller> listeners;
  private final Map<String, T> propertyCache;
  private final ExecutorService listenerExecutor;
  private final AtomicBoolean closed;

  protected AbstractPropertyStore() {
    this.listeners = LinkedHashMultimap.create();
    this.propertyCache = Maps.newHashMap();
    this.listenerExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("property-store-%d"));
    this.closed = new AtomicBoolean();
  }

  @Override
  public final synchronized Cancellable addChangeListener(String name, PropertyChangeListener<T> listener) {
    ListenerCaller caller = new ListenerCaller(name, listener);
    listeners.put(name, caller);
    if (listenerAdded(name)) {
      T property = propertyCache.get(name);
      if (property != null) {
        caller.onChange(name, property);
      }
    }
    return caller;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      listenerExecutor.shutdownNow();
    }
  }

  protected boolean isClosed() {
    return closed.get();
  }

  /**
   * Invoked when a new listener is added.
   *
   * @param name Name of the property with listener added
   * @return {@code true} if notify the newly added listener with the cached value, {@code false} otherwise
   */
  protected abstract boolean listenerAdded(String name);

  /**
   * Retrieves the cached property value.
   *
   * @param name Name of the property
   * @return The cached value or {@code null} if no value is cached.
   */
  @Nullable
  protected final synchronized T getCached(String name) {
    return propertyCache.get(name);
  }

  /**
   * Updates property value and notify listeners.
   *
   * @param name Name of the property
   * @param property New value of the property
   */
  protected final synchronized void updateAndNotify(String name, T property) {
    if (isClosed()) {
      return;
    }

    if (property == null) {
      propertyCache.remove(name);
    } else {
      propertyCache.put(name, property);
    }

    for (ListenerCaller caller : listeners.get(name)) {
      caller.onChange(name, property);
    }
  }

  /**
   * Notifies listeners with error.
   *
   * @param name Name of the property
   * @param failureCause The cause of the error.
   */
  protected final synchronized void notifyError(String name, Throwable failureCause) {
    if (isClosed()) {
      return;
    }

    for (ListenerCaller caller : listeners.get(name)) {
      caller.onError(name, failureCause);
    }
  }

  /**
   * Helper class to make calls to actual change listener through an executor.
   */
  private final class ListenerCaller implements PropertyChangeListener<T>, Cancellable {

    private final String name;
    private final PropertyChangeListener<T> delegate;
    private volatile boolean cancelled;

    private ListenerCaller(String name, PropertyChangeListener<T> delegate) {
      this.delegate = delegate;
      this.name = name;
    }

    @Override
    public void onChange(final String name, final T property) {
      if (cancelled) {
        return;
      }
      try {
        listenerExecutor.execute(new Runnable() {
          @Override
          public void run() {
            if (cancelled) {
              return;
            }
            try {
              delegate.onChange(name, property);
            } catch (Throwable t) {
              invokeOnError(name, t);
            }
          }
        });
      } catch (RejectedExecutionException e) {
        // Executor already shutdown, meaning the property store service was stopped already.
      }
    }

    @Override
    public void onError(final String name, final Throwable failureCause) {
      if (cancelled) {
        return;
      }

      listenerExecutor.execute(new Runnable() {
        @Override
        public void run() {
          invokeOnError(name, failureCause);
        }
      });
    }

    @Override
    public void cancel() {
      cancelled = true;
      synchronized (AbstractPropertyStore.this) {
        listeners.remove(name, this);
      }
    }

    private void invokeOnError(String name, Throwable failureCause) {
      if (cancelled) {
        return;
      }
      try {
        delegate.onError(name, failureCause);
      } catch (Throwable t) {
        LOG.warn("Exception while calling PropertyChangeListener.onError", t);
      }
    }
  }
}
