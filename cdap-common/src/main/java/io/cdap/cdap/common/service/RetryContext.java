/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.common.service;

/**
 * Can be used to provide any required context to the {@link Retries.CallableWithContext}
 */
public class RetryContext {

  private final int retryAttempt;

  private RetryContext(int retryAttempt) {
    this.retryAttempt = retryAttempt;
  }

  /**
   * Returns retry attempt
   *
   * @return integer indicating the attempt
   */
  public int getRetryAttempt() {
    return retryAttempt;
  }

  /**
   * Builder for {@link RetryContext}
   */
  public static class Builder {
    private int retryAttempt;

    /**
     * @param retryAttempt integer indicating the attempt
     * @return {@link Builder}
     */
    public Builder withRetryAttempt(int retryAttempt) {
      this.retryAttempt = retryAttempt;
      return this;
    }

    /**
     * @return {@link RetryContext}
     */
    public RetryContext build() {
      return new RetryContext(retryAttempt);
    }
  }
}
