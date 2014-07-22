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
import com.continuuity.explore.service.UnexpectedQueryStatusException;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A base for an Explore Client that talks to a server implementing {@link Explore} over HTTP.
 */
public abstract class AbstractExploreClient extends ExploreHttpClient implements ExploreClient {
  private StatementExecutor executor;
  private final int executorThreads;

  protected AbstractExploreClient(int executorThreads) {
    this.executorThreads = executorThreads;
  }

  @Override
  protected void startUp() throws Exception {
    if (!isAvailable()) {
      // If Explore service is not available, then we don't start the Explore client as well
      this.stopAndWait();
    }
    executor = new StatementExecutor(MoreExecutors.listeningDecorator(
      Executors.newFixedThreadPool(executorThreads, Threads.createDaemonThreadFactory("explore-client-executor"))));
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  public ListenableFuture<Void> disableExplore(final String datasetInstance) {
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
    final AbstractExploreClient client = this;
    StatementExecutionFuture future = executor.submit(new Callable<ExecutionResults>() {
      @Override
      public ExecutionResults call() throws Exception {
        Handle handle = futureHandle.get();

        Status status;
        do {
          TimeUnit.MILLISECONDS.sleep(300);
          status = client.getStatus(handle);
        } while (status.getStatus() == Status.OpStatus.RUNNING || status.getStatus() == Status.OpStatus.PENDING ||
          status.getStatus() == Status.OpStatus.INITIALIZED || status.getStatus() == Status.OpStatus.UNKNOWN);

        switch (status.getStatus()) {
          case FINISHED:
            if (!status.hasResults()) {
              client.close(handle);
            }
            return new ClientExecutionResults(client, handle, status.hasResults());
          default:
            throw new UnexpectedQueryStatusException("Error while running query.", status.getStatus());
        }
      }
    }, this, futureHandle);

    return future;
  }

  /**
   * Result iterator which polls Explore service using HTTP to get next results.
   */
  private static final class ClientExecutionResults implements ExecutionResults {
    private static final Logger LOG = LoggerFactory.getLogger(ClientExecutionResults.class);
    private static final int POLLING_SIZE = 100;

    private Iterator<Result> delegate;
    private boolean hasNext = true;

    private final ExploreHttpClient exploreClient;
    private final Handle handle;
    private final boolean mayHaveResults;

    public ClientExecutionResults(ExploreHttpClient exploreClient, Handle handle, boolean mayHaveResults) {
      this.exploreClient = exploreClient;
      this.handle = handle;
      this.mayHaveResults = mayHaveResults;
    }

    @Override
    public boolean hasNext() {
      if (!hasNext || !mayHaveResults) {
        return false;
      }

      if (delegate == null || !delegate.hasNext()) {
        try {
          // call the endpoint 'next' to get more results and set delegate
          List<Result> nextResults = exploreClient.nextResults(handle, POLLING_SIZE);
          delegate = nextResults.iterator();

          // At this point, if delegate has no result, there are no more results at all
          hasNext = delegate.hasNext();
          return hasNext;
        } catch (ExploreException e) {
          LOG.error("Exception while iterating through the results of query {}", handle.getHandle(), e);
          Throwables.propagate(e);
        } catch (HandleNotFoundException e) {
          // Handle may have timed out, or the handle given is just unknown
          LOG.debug("Received exception", e);
          hasNext = false;
          return false;
        }
      }
      // At this point we know that delegate.hasNext() is true
      return true;
    }

    @Override
    public Result next() {
      if (hasNext()) {
        return delegate.next();
      }
      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
