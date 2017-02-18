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

package co.cask.cdap.common.logging;

import co.cask.cdap.common.utils.TimeProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.spi.LocationAwareLogger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to provide common implementations of {@link LogSampler}.
 */
public final class LogSamplers {

  /**
   * Returns a {@link LogSampler} that accepts once on every N calls.
   */
  public static LogSampler onceEvery(final int count) {
    Preconditions.checkArgument(count > 0, "Count must be > 0");
    return new CountBasedLogSampler() {
      @Override
      protected boolean accept(String message, int logLevel, long callCount) {
        return (callCount - 1) % count == 0;
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that accepts once on every N calls, with N exponentially increased from
   * an initial value to a max value.
   */
  public static LogSampler exponentialLimit(final int initialCount, final int maxCount, final double multiplier) {
    Preconditions.checkArgument(initialCount > 0, "Initial count must be >= 0");
    Preconditions.checkArgument(maxCount > 0, "Max count must be >= 0");
    Preconditions.checkArgument(multiplier >= 1.0d, "Multiplier must be >= 1.0");

    final AtomicDouble modular = new AtomicDouble(initialCount);

    return new CountBasedLogSampler() {
      @Override
      protected boolean accept(String message, int logLevel, long callCount) {
        double mod = modular.get();
        if (((callCount - 1) % Math.ceil(mod)) == 0) {
          modular.compareAndSet(mod, Math.min(mod * multiplier, maxCount));
          return true;
        }
        return false;
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that accepts once as per the frequency. This method is equivalent to
   * calling {@link #limitRate(long, TimeProvider)} using {@link TimeProvider#SYSTEM_TIME} as the
   * {@link TimeProvider}.
   */
  public static LogSampler limitRate(long frequency) {
    return limitRate(frequency, TimeProvider.SYSTEM_TIME);
  }

  /**
   * Returns a {@link LogSampler} that accepts once as per the frequency. The
   *
   * @param frequency frequency in milliseconds
   * @param timeProvider a {@link TimeProvider} to provide the current time in milliseconds
   */
  public static LogSampler limitRate(final long frequency, TimeProvider timeProvider) {
    Preconditions.checkArgument(frequency >= 0, "Frequency must be >= 0");
    return new TimeBasedLogSampler(timeProvider) {
      @Override
      protected long computeNextLogTime(String message, int logLevel, long currentLogTime) {
        return currentLogTime + frequency;
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log message matches with one of
   * the provided messages. This method is equivalent to calling {@link #onMessages(LogSampler, Iterable)}
   * by converting the array of messages to a list.
   */
  public static LogSampler onMessages(LogSampler sampler, String...messages) {
    return onMessages(sampler, Arrays.asList(messages));
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log message matches with one of
   * the provided messages.
   *
   * @param sampler the {@link LogSampler} to call if the log message matches
   * @param messages the list of messages to match
   */
  public static LogSampler onMessages(final LogSampler sampler, Iterable<String> messages) {
    final Set<String> sampleOnMessages = new HashSet<>();
    Iterables.addAll(sampleOnMessages, messages);
    return new LogSampler() {
      @Override
      public boolean accept(String message, int logLevel) {
        // If the message is not in the sample set, let it pass
        return !sampleOnMessages.contains(message) || sampler.accept(message, logLevel);
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log message matches with the regex {@link Pattern}.
   *
   * @param sampler the {@link LogSampler} to call if the log message matches
   * @param pattern the regex pattern
   * @param matchAll {@code true} to match the pattern with the whole message; {@code false} to match any sub-sequence
   */
  public static LogSampler onPattern(final LogSampler sampler, final Pattern pattern, final boolean matchAll) {
    return new LogSampler() {
      @Override
      public boolean accept(String message, int logLevel) {
        Matcher matcher = pattern.matcher(message);
        // If the message doesn't match the pattern, let it pass
        return !(matchAll ? matcher.matches() : matcher.find()) || sampler.accept(message, logLevel);
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log level is trace.
   */
  public static LogSampler onTrace(LogSampler sampler) {
    return onLogLevel(sampler, LocationAwareLogger.TRACE_INT);
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log level is debug or lower.
   */
  public static LogSampler onDebug(LogSampler sampler) {
    return onLogLevel(sampler, LocationAwareLogger.DEBUG_INT);
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log level is info or lower.
   */
  public static LogSampler onInfo(LogSampler sampler) {
    return onLogLevel(sampler, LocationAwareLogger.INFO_INT);
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log level is warn or lower.
   */
  public static LogSampler onWarn(LogSampler sampler) {
    return onLogLevel(sampler, LocationAwareLogger.WARN_INT);
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the log level is error or lower.
   */
  public static LogSampler onError(LogSampler sampler) {
    return onLogLevel(sampler, LocationAwareLogger.ERROR_INT);
  }

  /**
   * Returns a {@link LogSampler} that will accept a log event if any of the provided {@link LogSampler}s accepted
   * the log event.
   */
  public static LogSampler any(final LogSampler...samplers) {
    Preconditions.checkArgument(samplers.length > 0, "Must provide at least one sampler");
    return new LogSampler() {
      @Override
      public boolean accept(String message, int logLevel) {
        boolean accept = false;

        for (LogSampler sampler : samplers) {
          // Intentionally not to short-circuit to make sure all samplers get called in order to mutate states
          accept = sampler.accept(message, logLevel) || accept;
        }
        return accept;
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that will accept a log event if all of the provided {@link LogSampler}s accepted
   * the log event.
   */
  public static LogSampler all(final LogSampler...samplers) {
    Preconditions.checkArgument(samplers.length > 0, "Must provide at least one sampler");
    return new LogSampler() {
      @Override
      public boolean accept(String message, int logLevel) {
        boolean accept = true;
        for (LogSampler sampler : samplers) {
          // Intentionally not to short-circuit to make sure all samplers get called in order to mutate states
          accept = sampler.accept(message, logLevel) && accept;
        }
        return accept;
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that uses an independent {@link LogSampler} per each message.
   */
  public static LogSampler perMessage(final Supplier<LogSampler> samplerSupplier) {
    return new LogSampler() {

      private final ConcurrentMap<String, LogSampler> messageSamplers = new ConcurrentHashMap<>();

      @Override
      public boolean accept(String message, int logLevel) {
        LogSampler logSampler = messageSamplers.get(message);
        if (logSampler == null) {
          LogSampler newLogSampler = samplerSupplier.get();
          logSampler = messageSamplers.putIfAbsent(message, newLogSampler);
          if (logSampler == null) {
            logSampler = newLogSampler;
          }
        }
        return logSampler.accept(message, logLevel);
      }
    };
  }

  /**
   * Returns a {@link LogSampler} that only perform sampling if the event log level is smaller than or equal to
   * the provided log level.
   */
  private static LogSampler onLogLevel(final LogSampler sampler, final int level) {
    return new LogSampler() {
      @Override
      public boolean accept(String message, int logLevel) {
        return level < logLevel || sampler.accept(message, logLevel);
      }
    };
  }

  private LogSamplers() {
    // no-op
  }
}
