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

package com.google.common.base;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Shadow compatibility class for Guava's Stopwatch to preserve compatibility
 * with precompiled dependencies (like Apache Tephra) under Guava 32.
 */
public final class Stopwatch {
  private final Ticker ticker;
  private boolean isRunning;
  private long elapsedNanos;
  private long startTick;

  public static Stopwatch createUnstarted() {
    return new Stopwatch();
  }

  public static Stopwatch createUnstarted(Ticker ticker) {
    return new Stopwatch(ticker);
  }

  public static Stopwatch createStarted() {
    return new Stopwatch().start();
  }

  public static Stopwatch createStarted(Ticker ticker) {
    return new Stopwatch(ticker).start();
  }

  /**
   * Legacy constructor made public for binary compatibility with precompiled libraries.
   */
  public Stopwatch() {
    this.ticker = Ticker.systemTicker();
  }

  /**
   * Legacy constructor made public for binary compatibility with precompiled libraries.
   */
  public Stopwatch(Ticker ticker) {
    this.ticker = Preconditions.checkNotNull(ticker, "ticker");
  }

  public boolean isRunning() {
    return isRunning;
  }

  public Stopwatch start() {
    Preconditions.checkState(!isRunning, "This stopwatch is already running.");
    isRunning = true;
    startTick = ticker.read();
    return this;
  }

  public Stopwatch stop() {
    long tick = ticker.read();
    Preconditions.checkState(isRunning, "This stopwatch is already stopped.");
    isRunning = false;
    elapsedNanos += tick - startTick;
    return this;
  }

  public Stopwatch reset() {
    elapsedNanos = 0;
    isRunning = false;
    return this;
  }

  private long elapsedNanos() {
    return isRunning ? ticker.read() - startTick + elapsedNanos : elapsedNanos;
  }

  public long elapsed(TimeUnit timeUnit) {
    return timeUnit.convert(elapsedNanos(), TimeUnit.NANOSECONDS);
  }

  public Duration elapsed() {
    return Duration.ofNanos(elapsedNanos());
  }

  // --- Legacy methods for backward compatibility ---

  @Deprecated
  public long elapsedTime(TimeUnit timeUnit) {
    return elapsed(timeUnit);
  }

  @Deprecated
  public long elapsedMillis() {
    return elapsed(TimeUnit.MILLISECONDS);
  }

  @Override
  public String toString() {
    long nanos = elapsedNanos();
    TimeUnit unit = chooseUnit(nanos);
    double value = (double) nanos / TimeUnit.NANOSECONDS.convert(1, unit);
    return String.format("%.4g %s", value, abbreviate(unit));
  }

  private static TimeUnit chooseUnit(long nanos) {
    if (TimeUnit.DAYS.convert(nanos, TimeUnit.NANOSECONDS) > 0) {
      return TimeUnit.DAYS;
    }
    if (TimeUnit.HOURS.convert(nanos, TimeUnit.NANOSECONDS) > 0) {
      return TimeUnit.HOURS;
    }
    if (TimeUnit.MINUTES.convert(nanos, TimeUnit.NANOSECONDS) > 0) {
      return TimeUnit.MINUTES;
    }
    if (TimeUnit.SECONDS.convert(nanos, TimeUnit.NANOSECONDS) > 0) {
      return TimeUnit.SECONDS;
    }
    if (TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS) > 0) {
      return TimeUnit.MILLISECONDS;
    }
    if (TimeUnit.MICROSECONDS.convert(nanos, TimeUnit.NANOSECONDS) > 0) {
      return TimeUnit.MICROSECONDS;
    }
    return TimeUnit.NANOSECONDS;
  }

  private static String abbreviate(TimeUnit unit) {
    switch (unit) {
      case NANOSECONDS:
        return "ns";
      case MICROSECONDS:
        return "μs";
      case MILLISECONDS:
        return "ms";
      case SECONDS:
        return "s";
      case MINUTES:
        return "min";
      case HOURS:
        return "h";
      case DAYS:
        return "d";
      default:
        throw new AssertionError();
    }
  }
}
