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

package co.cask.cdap.common.service;

/**
 * Types of retry strategies that users can configure for programs.
 */
public enum RetryStrategyType {
  NONE("none"),
  FIXED_DELAY("fixed.delay"),
  EXPONENTIAL_BACKOFF("exponential.backoff");

  private final String id;

  RetryStrategyType(String id) {
    this.id = id;
  }

  public static RetryStrategyType from(String str) {
    switch (str.toLowerCase()) {
      case "none":
        return NONE;
      case "fixed.delay":
        return FIXED_DELAY;
      case "exponential.backoff":
        return EXPONENTIAL_BACKOFF;
      default:
        throw new IllegalArgumentException(String.format("Unknown RetryStrategyType %s. Must be one of %s.",
                                                         str, "none, fixed.delay, or exponential.backoff"));
    }
  }

  @Override
  public String toString() {
    return id;
  }
}
