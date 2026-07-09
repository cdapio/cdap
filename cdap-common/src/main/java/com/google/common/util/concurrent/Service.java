/*
 * Copyright © 2026 Cask Data, Inc.
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

package com.google.common.util.concurrent;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Shadow compatibility interface for Guava Service to preserve compatibility with precompiled
 * dependencies (like Apache Tephra) under Guava 32.
 */
public interface Service {
  default Service startAsync() {
    try {
      boolean implementsStart = false;
      Class<?> clazz = this.getClass();
      while (clazz != null && clazz != Object.class) {
        try {
          clazz.getDeclaredMethod("start");
          implementsStart = true;
          break;
        } catch (NoSuchMethodException e) {
          clazz = clazz.getSuperclass();
        }
      }
      if (implementsStart) {
        try {
          addListener(new Listener() {
            @Override
            public void failed(State from, Throwable failure) {
              FailureTracker.setFailure(Service.this, failure);
            }
          }, MoreExecutors.directExecutor());
        } catch (Exception e) {
          // ignore
        }
        start();
        return this;
      }
    } catch (Exception e) {
      // fallback
    }
    throw new UnsupportedOperationException("startAsync() is not implemented by " + this.getClass().getName());
  }

  boolean isRunning();
  State state();

  default Service stopAsync() {
    try {
      boolean implementsStop = false;
      Class<?> clazz = this.getClass();
      while (clazz != null && clazz != Object.class) {
        try {
          clazz.getDeclaredMethod("stop");
          implementsStop = true;
          break;
        } catch (NoSuchMethodException e) {
          clazz = clazz.getSuperclass();
        }
      }
      if (implementsStop) {
        stop();
        return this;
      }
    } catch (Exception e) {
      // fallback
    }
    throw new UnsupportedOperationException("stopAsync() is not implemented by " + this.getClass().getName());
  }

  default void awaitRunning() {
    State currentState = state();
    if (currentState == State.RUNNING) {
      return;
    }
    if (currentState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    if (currentState == State.TERMINATED || currentState == State.STOPPING) {
      throw new IllegalStateException("Service already stopped or stopping");
    }
    
    final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    final Throwable[] failedThrowable = new Throwable[1];
    addListener(new Listener() {
      @Override
      public void running() {
        latch.countDown();
      }
      @Override
      public void failed(State from, Throwable failure) {
        failedThrowable[0] = failure;
        latch.countDown();
      }
    }, MoreExecutors.directExecutor());
    
    State recheckState = state();
    if (recheckState == State.RUNNING) {
      return;
    }
    if (recheckState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    
    if (failedThrowable[0] != null) {
      throw new IllegalStateException("Service failed", failedThrowable[0]);
    }
  }

  default void awaitRunning(Duration timeout) throws TimeoutException {
    awaitRunning(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  default void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {
    State currentState = state();
    if (currentState == State.RUNNING) {
      return;
    }
    if (currentState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    if (currentState == State.TERMINATED || currentState == State.STOPPING) {
      throw new IllegalStateException("Service already stopped or stopping");
    }
    
    final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    final Throwable[] failedThrowable = new Throwable[1];
    addListener(new Listener() {
      @Override
      public void running() {
        latch.countDown();
      }
      @Override
      public void failed(State from, Throwable failure) {
        failedThrowable[0] = failure;
        latch.countDown();
      }
    }, MoreExecutors.directExecutor());
    
    State recheckState = state();
    if (recheckState == State.RUNNING) {
      return;
    }
    if (recheckState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    
    try {
      if (!latch.await(timeout, unit)) {
        throw new TimeoutException("Timed out waiting for service to run");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    
    if (failedThrowable[0] != null) {
      throw new IllegalStateException("Service failed", failedThrowable[0]);
    }
  }

  default void awaitTerminated() {
    State currentState = state();
    if (currentState == State.TERMINATED) {
      return;
    }
    if (currentState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    
    final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    final Throwable[] failedThrowable = new Throwable[1];
    addListener(new Listener() {
      @Override
      public void terminated(State from) {
        latch.countDown();
      }
      @Override
      public void failed(State from, Throwable failure) {
        failedThrowable[0] = failure;
        latch.countDown();
      }
    }, MoreExecutors.directExecutor());
    
    State recheckState = state();
    if (recheckState == State.TERMINATED) {
      return;
    }
    if (recheckState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    
    if (failedThrowable[0] != null) {
      throw new IllegalStateException("Service failed", failedThrowable[0]);
    }
  }

  default void awaitTerminated(Duration timeout) throws TimeoutException {
    awaitTerminated(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  default void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
    State currentState = state();
    if (currentState == State.TERMINATED) {
      return;
    }
    if (currentState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    
    final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
    final Throwable[] failedThrowable = new Throwable[1];
    addListener(new Listener() {
      @Override
      public void terminated(State from) {
        latch.countDown();
      }
      @Override
      public void failed(State from, Throwable failure) {
        failedThrowable[0] = failure;
        latch.countDown();
      }
    }, MoreExecutors.directExecutor());
    
    State recheckState = state();
    if (recheckState == State.TERMINATED) {
      return;
    }
    if (recheckState == State.FAILED) {
      throw new IllegalStateException("Service failed", failureCause());
    }
    
    try {
      if (!latch.await(timeout, unit)) {
        throw new TimeoutException("Timed out waiting for service to terminate");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    
    if (failedThrowable[0] != null) {
      throw new IllegalStateException("Service failed", failedThrowable[0]);
    }
  }

  default Throwable failureCause() {
    Throwable t = FailureTracker.getFailure(this);
    if (t != null) {
      return t;
    }
    return new IllegalStateException("Service failed with unknown cause");
  }

  void addListener(Listener listener, Executor executor);

  enum State {
    NEW,
    STARTING,
    RUNNING,
    STOPPING,
    TERMINATED,
    FAILED
  }

  abstract class Listener {
    public Listener() {}
    public void starting() {}
    public void running() {}
    public void stopping(State from) {}
    public void terminated(State from) {}
    public void failed(State from, Throwable failure) {}
  }

  // --- Legacy backward compatibility default methods ---

  default ListenableFuture<State> start() {
    final SettableFuture<State> future = SettableFuture.create();
    addListener(new Listener() {
      @Override
      public void running() {
        future.set(State.RUNNING);
      }

      @Override
      public void failed(State from, Throwable failure) {
        future.setException(failure);
      }
    }, MoreExecutors.directExecutor());
    startAsync();
    return future;
  }

  default State startAndWait() {
    startAsync().awaitRunning();
    return state();
  }

  default ListenableFuture<State> stop() {
    final SettableFuture<State> future = SettableFuture.create();
    addListener(new Listener() {
      @Override
      public void terminated(State from) {
        future.set(State.TERMINATED);
      }

      @Override
      public void failed(State from, Throwable failure) {
        future.setException(failure);
      }
    }, MoreExecutors.directExecutor());
    stopAsync();
    return future;
  }

  default State stopAndWait() {
    stopAsync().awaitTerminated();
    return state();
  }

  class FailureTracker {
    private static final java.util.Map<Service, Throwable> failures = 
        java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    public static void setFailure(Service service, Throwable failure) {
      failures.put(service, failure);
    }

    public static Throwable getFailure(Service service) {
      return failures.get(service);
    }
  }
}
