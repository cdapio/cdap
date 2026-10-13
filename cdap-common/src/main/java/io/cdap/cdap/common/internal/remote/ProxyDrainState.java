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

package io.cdap.cdap.common.internal.remote;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the task requests the proxy has taken on, so that shutting down can wait until their
 * responses have been delivered instead of cutting them off.
 *
 * <p>A request counts from the moment it passes the drain check until its App Fabric connection
 * closes (or until it is rejected before connecting to a worker). The proxy carries one task
 * request per connection and closes the connection once the response has been flushed, so a closed
 * connection means the response was delivered or the request failed.
 */
final class ProxyDrainState {

  private static final Logger LOG = LoggerFactory.getLogger(ProxyDrainState.class);
  private static final long PROGRESS_LOG_INTERVAL_MILLIS = 5000L;

  private final Object lock = new Object();
  private int inFlight;
  private volatile boolean draining;

  /** Whether shutdown has started; new task requests must be turned away. */
  boolean isDraining() {
    return draining;
  }

  /**
   * Atomically checks that the proxy is not draining and increments the in-flight count.
   *
   * @return {@code true} if the request was admitted, {@code false} if the proxy is draining
   */
  boolean tryStartRequest() {
    synchronized (lock) {
      if (draining) {
        return false;
      }
      inFlight++;
      return true;
    }
  }

  /** Called when a request is given a task worker slot. */
  void requestStarted() {
    synchronized (lock) {
      inFlight++;
    }
  }

  /** Called exactly once per started request, when its App Fabric connection closes. */
  void requestFinished() {
    synchronized (lock) {
      inFlight--;
      if (inFlight == 0) {
        lock.notifyAll();
      }
    }
  }

  int getInFlight() {
    synchronized (lock) {
      return inFlight;
    }
  }

  /** Stops new task requests from being taken on. */
  void startDraining() {
    synchronized (lock) {
      draining = true;
    }
  }

  /**
   * Waits until no request is in flight, or until {@code timeoutMillis} passes. Must not run on a
   * Netty event loop: those threads deliver the responses that bring the count down.
   *
   * @return {@code true} if every request finished, {@code false} if the timeout passed first
   */
  boolean awaitDrained(long timeoutMillis) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    synchronized (lock) {
      while (inFlight > 0) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        // Floor to 1ms while remainingNanos > 0 so sub-millisecond remainders don't call wait(0).
        long waitMillis = Math.max(1L,
            Math.min(TimeUnit.NANOSECONDS.toMillis(remainingNanos), PROGRESS_LOG_INTERVAL_MILLIS));
        // Woken by requestFinished() reaching 0; the periodic wake-up only logs progress.
        lock.wait(waitMillis);
        if (inFlight > 0) {
          LOG.info("Waiting for {} in-flight task worker request(s) before stopping", inFlight);
        }
      }
      return true;
    }
  }
}
