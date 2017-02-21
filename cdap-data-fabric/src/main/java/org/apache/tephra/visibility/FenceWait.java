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

package org.apache.tephra.visibility;

import org.apache.tephra.TransactionFailureException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Used by a writer to wait on a fence so that changes are visible to all readers with in-progress transactions.
 */
public interface FenceWait {
  /**
   * Waits until the fence is complete, or till the timeout specified. The fence wait transaction will get re-tried
   * several times until the timeout.
   * <p>
   *
   * If a fence wait times out then it means there are still some readers with in-progress transactions that have not
   * seen the change. In this case the wait will have to be retried using the same FenceWait object.
   *
   * @param timeout Maximum time to wait
   * @param timeUnit {@link TimeUnit} for timeout and sleepTime
   * @throws TransactionFailureException when not able to start fence wait transaction
   * @throws InterruptedException on any interrupt
   * @throws TimeoutException when timeout is reached
   */
  void await(long timeout, TimeUnit timeUnit)
    throws TransactionFailureException, InterruptedException, TimeoutException;
}
