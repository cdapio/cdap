/*
 * Copyright (C) 2009 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An object with an operational state, plus asynchronous {@link #startAsync()} and
 * {@link #stopAsync()} lifecycle methods to transition between states.
 *
 * <p>Includes backwards-compatible default methods for Guava 13.
 */
public interface Service {

  /**
   * If the service state is {@link State#NEW}, this initiates service startup and returns
   * immediately.
   *
   * @return this
   */
  Service startAsync();

  /**
   * Returns {@code true} if this service is {@linkplain State#RUNNING running}.
   */
  boolean isRunning();

  /**
   * Returns the lifecycle state of the service.
   */
  State state();

  /**
   * If the service is {@linkplain State#STARTING starting} or {@linkplain State#RUNNING running},
   * this initiates service shutdown and returns immediately.
   *
   * @return this
   */
  Service stopAsync();

  /**
   * Waits for the {@link Service} to reach the {@linkplain State#RUNNING running state}.
   */
  void awaitRunning();

  /**
   * Waits for the {@link Service} to reach the {@linkplain State#RUNNING running state} for no more
   * than the given time.
   */
  default void awaitRunning(Duration timeout) throws TimeoutException {
    awaitRunning(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  /**
   * Waits for the {@link Service} to reach the {@linkplain State#RUNNING running state} for no more
   * than the given time.
   */
  void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Waits for the {@link Service} to reach the {@linkplain State#TERMINATED terminated state}.
   */
  void awaitTerminated();

  /**
   * Waits for the {@link Service} to reach the {@linkplain State#TERMINATED terminated state} for
   * no more than the given time.
   */
  default void awaitTerminated(Duration timeout) throws TimeoutException {
    awaitTerminated(timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  /**
   * Waits for the {@link Service} to reach the {@linkplain State#TERMINATED terminated state} for
   * no more than the given time.
   */
  void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Returns the {@link Throwable} that caused this service to fail.
   */
  default Throwable failureCause() {
    return null;
  }

  /**
   * Registers a {@link Listener} to be executed on the given executor.
   */
  void addListener(Listener listener, Executor executor);

  /**
   * The lifecycle states of a service.
   */
  enum State {
    NEW,
    STARTING,
    RUNNING,
    STOPPING,
    TERMINATED,
    FAILED
  }

  /**
   * A listener for the various state changes that a {@link Service} goes through in its lifecycle.
   */
  abstract class Listener {
    public void starting() {
    }

    public void running() {
    }

    public void stopping(State from) {
    }

    public void terminated(State from) {
    }

    public void failed(State from, Throwable failure) {
    }
  }

  /**
   * Guava 13 backwards compatibility method.
   *
   * @return A completed future with the running state
   */
  default ListenableFuture<State> start() {
    if (state() == State.NEW) {
      startAsync().awaitRunning();
    } else if (state() == State.STARTING) {
      awaitRunning();
    }
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 backwards compatibility method.
   *
   * @return The state after starting
   */
  default State startAndWait() {
    if (state() == State.NEW) {
      startAsync().awaitRunning();
    } else if (state() == State.STARTING) {
      awaitRunning();
    }
    return state();
  }

  /**
   * Guava 13 backwards compatibility method.
   *
   * @return A completed future with the terminated state
   */
  default ListenableFuture<State> stop() {
    if (state() == State.RUNNING || state() == State.STARTING) {
      stopAsync().awaitTerminated();
    } else if (state() == State.STOPPING) {
      awaitTerminated();
    }
    return Futures.immediateFuture(state());
  }

  /**
   * Guava 13 backwards compatibility method.
   *
   * @return The state after stopping
   */
  default State stopAndWait() {
    if (state() == State.RUNNING || state() == State.STARTING) {
      stopAsync().awaitTerminated();
    } else if (state() == State.STOPPING) {
      awaitTerminated();
    }
    return state();
  }
}
