/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

/**
 * Retries transaction execution when transaction fails with {@link TransactionConflictException}.
 */
public class RetryOnConflictStrategy implements RetryStrategy {
  private final int maxRetries;
  private final long retryDelay;

  public RetryOnConflictStrategy(int maxRetries, long retryDelay) {
    this.maxRetries = maxRetries;
    this.retryDelay = retryDelay;
  }

  @Override
  public long nextRetry(TransactionFailureException reason, int failureCount) {
    if (reason instanceof TransactionConflictException) {
      return failureCount > maxRetries ? -1 : retryDelay;
    } else {
      return -1;
    }
  }
}
