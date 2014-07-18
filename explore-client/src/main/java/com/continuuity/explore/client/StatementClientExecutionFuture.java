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

import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.StatementExecutionFuture;
import com.continuuity.explore.service.UnexpectedQueryStatusException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Implementation of {@link StatementExecutionFuture} used in Explore client.
 *
 * @param <T> Type of the result object.
 */
public class StatementClientExecutionFuture<T> extends StatementExecutionFuture<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StatementClientExecutionFuture.class);

  private final BaseExploreClient exploreClient;
  private final ListenableFuture<Handle> futureHandle;

  protected StatementClientExecutionFuture(ListenableFuture<T> delegate, BaseExploreClient exploreClient,
                                           ListenableFuture<Handle> futureHandle) {
    super(delegate);
    this.exploreClient = exploreClient;
    this.futureHandle = futureHandle;
  }

  @Override
  public List<ColumnDesc> getResultSchema() throws ExploreException, HandleNotFoundException {
    try {
      Handle handle = futureHandle.get();
      return exploreClient.doGetResultSchema(handle);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Throwables.propagate(e);
      return null;
    } catch (ExecutionException e) {
      LOG.error("Caught exception", e);
      Throwable t = e.getCause();
      if (t instanceof ExploreException) {
        LOG.error("Error in operation execution", t);
        throw (ExploreException) t;
      } else if (t instanceof HandleNotFoundException) {
        LOG.error("Caught exception", e);
        throw (HandleNotFoundException) t;
      }
      throw new ExploreException(t);
    }
  }

  @Override
  public ListenableFuture<Handle> getFutureStatementHandle() {
    return futureHandle;
  }

  @Override
  public void close() throws ExploreException, HandleNotFoundException {
    try {
      Handle handle = futureHandle.get();
      exploreClient.doCancel(handle);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Throwables.propagate(e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception", e);
      Throwable t = e.getCause();
      if (t instanceof ExploreException) {
        LOG.error("Error in operation execution", t);
        throw (ExploreException) t;
      } else if (t instanceof HandleNotFoundException) {
        LOG.error("Caught exception", e);
        throw (HandleNotFoundException) t;
      }
      throw new ExploreException(t);
    }
  }

  @Override
  public void cancel() throws ExploreException, HandleNotFoundException {
    try {
      Handle handle = futureHandle.get();
      exploreClient.doClose(handle);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Throwables.propagate(e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception", e);
      Throwable t = e.getCause();
      if (t instanceof ExploreException) {
        LOG.error("Error in operation execution", t);
        throw (ExploreException) t;
      } else if (t instanceof HandleNotFoundException) {
        LOG.error("Caught exception", e);
        throw (HandleNotFoundException) t;
      }
      throw new ExploreException(t);
    }
  }

}
