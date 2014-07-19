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
import com.continuuity.explore.service.Result;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Statement execution future implementation. The overridden methods use an {@link ExploreHttpClient}
 * instance internally.
 */
class StatementExecutionFutureImpl extends StatementExecutionFuture {
  private static final Logger LOG = LoggerFactory.getLogger(StatementExecutionFutureImpl.class);

  private final ExploreHttpClient exploreClient;
  private final ListenableFuture<Handle> futureHandle;

  StatementExecutionFutureImpl(ListenableFuture<Iterator<Result>> delegate, ExploreHttpClient exploreClient,
                               ListenableFuture<Handle> futureHandle) {
    super(delegate);
    this.exploreClient = exploreClient;
    this.futureHandle = futureHandle;
  }

  @Override
  public List<ColumnDesc> getResultSchema() throws ExploreException, HandleNotFoundException {
    try {
      Handle handle = futureHandle.get();
      return exploreClient.getResultSchema(handle);
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
      exploreClient.close(handle);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Throwables.propagate(e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception", e);
      Throwable t = e.getCause();
      if (t instanceof ExploreException) {
        LOG.error("Error when closing the execution", t);
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
      exploreClient.cancel(handle);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Throwables.propagate(e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception", e);
      Throwable t = e.getCause();
      if (t instanceof ExploreException) {
        LOG.error("Error when cancelling the execution", t);
        throw (ExploreException) t;
      } else if (t instanceof HandleNotFoundException) {
        LOG.error("Caught exception", e);
        throw (HandleNotFoundException) t;
      }
      throw new ExploreException(t);
    }
  }
}
