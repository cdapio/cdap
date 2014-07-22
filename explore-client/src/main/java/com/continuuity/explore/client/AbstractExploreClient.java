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

package com.continuuity.explore.client;

import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A base for an Explore Client that talks to a server implementing {@link Explore} over HTTP.
 */
public abstract class AbstractExploreClient extends ExploreHttpClient implements ExploreClient {
  private ListeningScheduledExecutorService executor;

  protected AbstractExploreClient() {
    executor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("explore-client-executor")));
  }

  @Override
  public void close() throws IOException {
    if (executor != null) {
      // This will cancel all the running tasks, with interruption - that means that all
      // queries submitted by this executor will be closed
      executor.shutdownNow();
    }
  }

  @Override
  public boolean isServiceAvailable() {
    return isAvailable();
  }

  @Override
  public ListenableFuture<Void> disableExplore(final String datasetInstance) {
    // NOTE: here we have two levels of Future because we want to return the future that actually
    // finishes the execution of the disable operation - it is not enough that the future handle
    // be available
    final ListenableFuture<Handle> futureHandle = executor.submit(new Callable<Handle>() {
      @Override
      public Handle call() throws Exception {
        return doDisableExplore(datasetInstance);
      }
    });
    StatementExecutionFuture futureResults = getFutureResultsFromHandle(futureHandle);

    // Exceptions will be thrown in case of an error in the futureHandle
    return Futures.transform(futureResults, Functions.<Void>constant(null));
  }

  @Override
  public ListenableFuture<Void> enableExplore(final String datasetInstance) {
    // NOTE: here we have two levels of Future because we want to return the future that actually
    // finishes the execution of the enable operation - it is not enough that the future handle
    // be available
    final ListenableFuture<Handle> futureHandle = executor.submit(new Callable<Handle>() {
      @Override
      public Handle call() throws Exception {
        return doEnableExplore(datasetInstance);
      }
    });
    StatementExecutionFuture futureResults = getFutureResultsFromHandle(futureHandle);

    // Exceptions will be thrown in case of an error in the futureHandle
    return Futures.transform(futureResults, Functions.<Void>constant(null));
  }

  @Override
  public StatementExecutionFuture submit(final String statement) {
    final ListenableFuture<Handle> futureHandle = executor.submit(new Callable<Handle>() {
      @Override
      public Handle call() throws Exception {
        return execute(statement);
      }
    });
    return getFutureResultsFromHandle(futureHandle);
  }

  /**
   * Create a {@link StatementExecutionFuture} object by polling the Explore service using the
   * {@link ListenableFuture} containing a {@link Handle}.
   */
  private StatementExecutionFuture getFutureResultsFromHandle(
    final ListenableFuture<Handle> futureHandle) {
    final StatementExecutionFutureImpl resultFuture = new StatementExecutionFutureImpl(this, futureHandle);
    Futures.addCallback(futureHandle, new FutureCallback<Handle>() {
      @Override
      public void onSuccess(final Handle handle) {
        try {
          Status status = getStatus(handle);
          if (!status.getStatus().isFinished()) {
            executor.schedule(new Runnable() {
              @Override
              public void run() {
                onSuccess(handle);
              }
            }, 300, TimeUnit.MILLISECONDS);
          } else {
            if (!status.hasResults()) {
              close(handle);
            }
            resultFuture.set(new ClientExploreExecutionResult(AbstractExploreClient.this, handle, status.hasResults()));
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        resultFuture.setException(t);
      }
    }, executor);
    return resultFuture;
  }

  /**
   * Result iterator which polls Explore service using HTTP to get next results.
   */
  private static final class ClientExploreExecutionResult extends AbstractIterator<Result>
    implements ExploreExecutionResult {
    private static final Logger LOG = LoggerFactory.getLogger(ClientExploreExecutionResult.class);
    private static final int POLLING_SIZE = 100;

    private Iterator<Result> delegate;

    private final ExploreHttpClient exploreClient;
    private final Handle handle;
    private final boolean hasResults;

    public ClientExploreExecutionResult(ExploreHttpClient exploreClient, Handle handle, boolean hasResults) {
      this.exploreClient = exploreClient;
      this.handle = handle;
      this.hasResults = hasResults;
    }

    @Override
    protected Result computeNext() {
      if (!hasResults) {
        return endOfData();
      }

      if (delegate != null && delegate.hasNext()) {
        return delegate.next();
      }
      try {
        // call the endpoint 'next' to get more results and set delegate
        List<Result> nextResults = exploreClient.nextResults(handle, POLLING_SIZE);
        delegate = nextResults.iterator();

        // At this point, if delegate has no result, there are no more results at all
        if (!delegate.hasNext()) {
          return endOfData();
        }
        return delegate.next();
      } catch (ExploreException e) {
        LOG.error("Exception while iterating through the results of query {}", handle.getHandle(), e);
        throw Throwables.propagate(e);
      } catch (HandleNotFoundException e) {
        // Handle may have timed out, or the handle given is just unknown
        LOG.debug("Received exception", e);
        return endOfData();
      }
    }

    @Override
    public void close() throws IOException {
      try {
        exploreClient.close(handle);
      } catch (HandleNotFoundException e) {
        // Don't need to throw an exception in that case - if the handle is not found, the query is already closed
        LOG.warn("Caught exception when closing the results", e);
      } catch (ExploreException e) {
        LOG.error("Caught exception during close operation", e);
        throw Throwables.propagate(e);
      }
    }
  }
}
