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
import com.continuuity.explore.service.ResultIterator;
import com.continuuity.explore.service.StatementExecutionFuture;
import com.continuuity.explore.service.Status;
import com.continuuity.explore.service.UnexpectedQueryStatusException;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A base for an Explore Client that talks to a server implementing {@link Explore} over HTTP.
 */
public abstract class BaseExploreClient extends ExploreHttpClient implements ExploreClient {
  private static final Logger LOG = LoggerFactory.getLogger(BaseExploreClient.class);

  private final StatementExecutor executor = new StatementExecutor(
    MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(100)));

  @Override
  public boolean isAvailable() {
    return super.isAvailable();
  }

  @Override
  public ListenableFuture<Boolean> disableExplore(final String datasetInstance) {
    final ListenableFuture<Handle> futureHandle = executor.submit(new Callable<Handle>() {
      @Override
      public Handle call() throws Exception {
        return doDisableExplore(datasetInstance);
      }
    });
    StatementExecutionFuture<ResultIterator> futureResults = getFutureResultsFromHandle(futureHandle);

    return Futures.transform(futureResults, new Function<ResultIterator, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable ResultIterator input) {
        // TODO really wondering how to deal with exceptions here...
        // We actually never return false - exceptions will be thrown in case of an error
        return true;
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> enableExplore(final String datasetInstance) {
    final ListenableFuture<Handle> futureHandle = executor.submit(new Callable<Handle>() {
      @Override
      public Handle call() throws Exception {
        return doEnableExplore(datasetInstance);
      }
    });
    StatementExecutionFuture<ResultIterator> futureResults = getFutureResultsFromHandle(futureHandle);

    return Futures.transform(futureResults, new Function<ResultIterator, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable ResultIterator input) {
        // We actually never return false - exceptions will be thrown in case of an error
        return true;
      }
    });
  }

  @Override
  public StatementExecutionFuture<ResultIterator> execute(final String statement) {
    final ListenableFuture<Handle> futureHandle = executor.submit(new Callable<Handle>() {
      @Override
      public Handle call() throws Exception {
        // TODO What do we do here with the exception, where is it handled then?
        // The people who receive the listenable future can use CheckedFuture to handle it
        // see https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained#CheckedFuture
        return doExecute(statement);
      }
    });
    return getFutureResultsFromHandle(futureHandle);
  }

  /**
   * Create a {@link StatementExecutionFuture} object by polling the Explore service using the
   * {@link ListenableFuture} containing a {@link Handle}.
   */
  private StatementExecutionFuture<ResultIterator> getFutureResultsFromHandle(
    final ListenableFuture<Handle> futureHandle) {
    final BaseExploreClient client = this;
    StatementExecutionFuture<ResultIterator> future = executor.submit(new Callable<ResultIterator>() {
      @Override
      public ResultIterator call() throws Exception {
        Handle handle = futureHandle.get();

        Status status;
        do {
          TimeUnit.MILLISECONDS.sleep(300);
          status = client.doGetStatus(handle);
        } while (status.getStatus() == Status.OpStatus.RUNNING || status.getStatus() == Status.OpStatus.PENDING ||
          status.getStatus() == Status.OpStatus.INITIALIZED || status.getStatus() == Status.OpStatus.UNKNOWN);

        switch (status.getStatus()) {
          case FINISHED:
            if (!status.hasResults()) {
              client.doClose(handle);
            }
            return new ResultIteratorClient(client, handle, status.hasResults());
          default:
            throw new UnexpectedQueryStatusException("Error while running query.", status.getStatus());
        }
      }
    }, this, futureHandle);

    return future;
  }

  /**
   * Result iterator which polls Explore service using HTTP to get next results.
   * TODO maybe we should take this class out of here
   */
  public static final class ResultIteratorClient implements ResultIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ResultIteratorClient.class);
    private static final int POLLING_SIZE = 100;

    private ResultIterator delegate;  // TODO test if it goes, otherwise change to Iterator<Result>
    private boolean hasNext = true;

    private final BaseExploreClient exploreClient;
    private final Handle handle;
    private final boolean mayHaveResults;

    public ResultIteratorClient(BaseExploreClient exploreClient, Handle handle, boolean mayHaveResults) {
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
          List<Result> nextResults = exploreClient.doNextResults(handle, POLLING_SIZE);
          delegate = (ResultIterator) nextResults.iterator();

          // At this point, if delegate has no result, there are no more results at all
          hasNext = delegate.hasNext();
          if (!hasNext) {
            LOG.trace("Closing query {} after fetching last results.", handle.getHandle());
            exploreClient.doClose(handle);
          }
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
