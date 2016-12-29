/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.retry;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Defines retry behavior when CDAP encounters an error during a retry-able operation.
 * Users should use the {@link #builder()} to create a RetryPolicy.
 */
public class RetryPolicy {
  public static final RetryPolicy NONE = new RetryPolicy(Type.NONE, null, null, null);
  private final Type type;
  private final Long baseDelay;
  private final Long maxDelay;
  private final Long timeout;

  /**
   * Types of retry policies.
   */
  public enum Type {
    NONE,
    FIXED_DELAY,
    EXPONENTIAL_BACKOFF
  }

  private RetryPolicy(@Nullable Type type, @Nullable Long timeout,
                      @Nullable Long baseDelay, @Nullable Long maxDelay) {
    this.type = type;
    this.timeout = timeout;
    this.baseDelay = baseDelay;
    this.maxDelay = maxDelay;
  }

  /**
   * @return the type of retry policy or null if the configured CDAP default should be used at runtime.
   */
  @Nullable
  public Type getType() {
    return type;
  }

  /**
   * @return the initial delay between retries in milliseconds
   * or null if the configured CDAP default should be used at runtime.
   */
  @Nullable
  public Long getBaseDelay() {
    return baseDelay;
  }

  /**
   * @return the maximum delay between retries in milliseconds
   * or null if the configured CDAP default should be used at runtime.
   */
  @Nullable
  public Long getMaxDelay() {
    return maxDelay;
  }

  /**
   * @return the maximum total time in milliseconds to retry before failing
   * or null if the configured CDAP default should be used at runtime.
   */
  @Nullable
  public Long getTimeout() {
    return timeout;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RetryPolicy that = (RetryPolicy) o;

    return type == that.type &&
      Objects.equals(baseDelay, that.baseDelay) &&
      Objects.equals(maxDelay, that.maxDelay) &&
      Objects.equals(timeout, that.timeout);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, baseDelay, maxDelay, timeout);
  }

  @Override
  public String toString() {
    return "RetryPolicy{" +
      "type=" + type +
      ", initialDelay=" + baseDelay +
      ", maxDelay=" + maxDelay +
      ", timeout=" + timeout +
      '}';
  }

  /**
   * @return a builder to create a retry policy using the configured CDAP default backoff type.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a RetryPolicy.
   */
  public static class Builder {
    protected Type type;
    protected Long baseDelay;
    protected Long maxDelay;
    protected Long timeout;

    /**
     * Set the type of retry policy. If none is given, the configured CDAP default will be used at runtime.
     *
     * @param type the type of retry policy
     * @return this builder
     */
    public Builder setType(Type type) {
      return this;
    }

    /**
     * Set the initial delay between retries. If none is given, the configured CDAP default will be used at runtime.
     *
     * @param duration initial delay between retries
     * @param timeUnit time unit of the duration. Must be in milliseconds or greater
     * @return this builder
     */
    public Builder setBaseDelay(long duration, TimeUnit timeUnit) {
      validateDuration("baseDelay", duration, timeUnit);
      baseDelay = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
      return this;
    }

    /**
     * Set the maximum delay between retries.
     * If none is given, the configured CDAP default will be used at runtime. Has no effect if the backoff
     * is {@link Type#FIXED_DELAY}.
     *
     * @param duration maximum delay between retries
     * @param timeUnit time unit of the duration. Must be in milliseconds or greater
     * @return this builder
     */
    public Builder setMaxDelay(long duration, TimeUnit timeUnit) {
      validateDuration("maxDelay", duration, timeUnit);
      maxDelay = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
      return this;
    }

    /**
     * Set the amount of time to retry before failing.
     * If none is given, the configured CDAP default will be used at runtime.
     *
     * @param duration amount of time to retry before failing
     * @param timeUnit time unit of the duration. Must be in milliseconds or greater
     * @return this builder
     */
    public Builder setTimeout(long duration, TimeUnit timeUnit) {
      validateDuration("timeout", duration, timeUnit);
      timeout = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
      return this;
    }

    protected void validateDuration(String property, long duration, TimeUnit timeUnit) {
      if (duration <= 0) {
        throw new IllegalArgumentException(
          String.format("Invalid %s of %d. %s must be a positive number.", property, duration, property));
      }
      if (timeUnit == TimeUnit.MICROSECONDS || timeUnit == TimeUnit.NANOSECONDS) {
        throw new IllegalArgumentException(
          String.format("Invalid time unit for %s of %s. Time units must be milliseconds or greater",
                        property, timeUnit));
      }
    }

    public RetryPolicy build() {
      // perform some sanity checks

      // the max delay should not be less than the initial delay
      if (maxDelay != null && baseDelay != null && maxDelay < baseDelay) {
        throw new IllegalArgumentException(String.format(
          "Invalid max (%d) and base (%d) delays. Max delay must be greater than or equal to initial delay.",
          maxDelay, baseDelay));
      }

      if (timeout != null && baseDelay != null && timeout < baseDelay) {
        throw new IllegalArgumentException(String.format(
          "Invalid initial delay (%d) and timeout (%d). The timeout must be greater than or equal to initial delay.",
          baseDelay, timeout));
      }

      return new RetryPolicy(type, timeout, baseDelay, maxDelay);
    }
  }

}
