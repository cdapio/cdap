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

import com.google.common.base.Stopwatch;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation of {@link FenceWait}.
 */
public class DefaultFenceWait implements FenceWait {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFenceWait.class);

  private final TransactionContext txContext;

  DefaultFenceWait(TransactionContext txContext) {
    this.txContext = txContext;
  }

  @Override
  public void await(long timeout, TimeUnit timeUnit)
    throws TransactionFailureException, InterruptedException, TimeoutException {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    long sleepTimeMicros = timeUnit.toMicros(timeout) / 10;
    // Have sleep time to be within 1 microsecond and 500 milliseconds
    sleepTimeMicros = Math.max(Math.min(sleepTimeMicros, 500 * 1000), 1);
    while (stopwatch.elapsedTime(timeUnit) < timeout) {
      txContext.start();
      try {
        txContext.finish();
        return;
      } catch (TransactionFailureException e) {
        LOG.error("Got exception waiting for fence. Sleeping for {} microseconds", sleepTimeMicros, e);
        txContext.abort();
        TimeUnit.MICROSECONDS.sleep(sleepTimeMicros);
      }
    }
    throw new TimeoutException("Timeout waiting for fence");
  }
}
