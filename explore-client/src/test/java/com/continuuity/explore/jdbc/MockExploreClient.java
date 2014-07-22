/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreExecutionResult;
import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.StatementExecutionFuture;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.Result;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Mock Explore client to use in test cases.
 */
public class MockExploreClient extends AbstractIdleService implements ExploreClient {

  private final Map<String, List<ColumnDesc>> statementsToMetadata;
  private final Map<String, List<Result>> statementsToResults;

  public MockExploreClient(Map<String, List<ColumnDesc>> statementsToMetadata,
                           Map<String, List<Result>> statementsToResults) {
    this.statementsToMetadata = Maps.newHashMap(statementsToMetadata);
    this.statementsToResults = Maps.newHashMap(statementsToResults);
  }

  @Override
  public ListenableFuture<Void> enableExplore(String datasetInstance) {
    return null;
  }

  @Override
  public ListenableFuture<Void> disableExplore(String datasetInstance) {
    return null;
  }

  @Override
  public StatementExecutionFuture submit(final String statement) {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get(statement).iterator()));
    return new StatementExecutionFuture(futureDelegate) {
      @Override
      public List<ColumnDesc> getResultSchema() throws ExploreException {
        return statementsToMetadata.get(statement);
      }

      @Override
      public ListenableFuture<Handle> getStatementHandleFuture() {
        SettableFuture<Handle> future = SettableFuture.create();
        future.set(Handle.fromId("foobar"));
        return future;
      }

      @Override
      public void close() throws ExploreException {
        statementsToMetadata.remove(statement);
        statementsToResults.remove(statement);
      }

      @Override
      protected boolean doCancel() {
        // TODO remove results for given handle
        return true;
      }
    };
  }

  @Override
  protected void startUp() throws Exception {
    // Do nothing
  }

  @Override
  protected void shutDown() throws Exception {
    // Do nothing
  }

  private static final class MockExploreExecutionResult implements ExploreExecutionResult {

    private final Iterator<Result> delegate;

    MockExploreExecutionResult(Iterator<Result> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Result next() {
      return delegate.next();
    }

    @Override
    public void remove() {
      delegate.remove();
    }
  }
}
