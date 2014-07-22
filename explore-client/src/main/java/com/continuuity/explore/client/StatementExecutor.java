/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.explore.client;

import com.continuuity.explore.service.Handle;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

/**
 * Executor service than delegates to another executor, and than can return
 * {@link StatementExecutionFuture} objects.
 */
public class StatementExecutor extends ForwardingListeningExecutorService {

  private final ListeningExecutorService delegate;

  public StatementExecutor(ListeningExecutorService delegate) {
    this.delegate = delegate;
  }

  @Override
  protected ListeningExecutorService delegate() {
    return delegate;
  }

  /**
   * Submit a callable to the executor. The execution will be delegated to another executor. The return
   * {@link com.google.common.util.concurrent.ListenableFuture} is of type {@link StatementExecutionFuture}.
   */
  public StatementExecutionFuture submit(Callable<ExploreExecutionResult> callable, ExploreHttpClient exploreClient,
                                         ListenableFuture<Handle> futureHandle) {
    ListenableFuture<ExploreExecutionResult> delegateFuture = super.submit(callable);
    return new StatementExecutionFutureImpl(delegateFuture, exploreClient, futureHandle);
  }
}
