/*
 * Copyright © 2015 Cask Data, Inc.
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
 * Provides strategy to use for operation retries.
 */
public interface RetryStrategy {

  /**
   * Returns the number of milliseconds to wait before retrying the operation.
   *
   * @param failures Number of times that the operation has been failed.
   * @param startTime Timestamp in milliseconds that the request starts.
   * @return Number of milliseconds to wait before retrying the operation. Returning {@code 0} means
   *     retry it immediately, while negative means abort the operation.
   */
  long nextRetry(int failures, long startTime);
}
