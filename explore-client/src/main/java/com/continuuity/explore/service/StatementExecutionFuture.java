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

package com.continuuity.explore.service;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * Future object that eventually contains the result of the execution of a statement by Explore.
 *
 * @param <T> Type of the result object.
 */
// TODO can remove the parameter T now, right? Let's see if there are other types than ResultIterator first
public abstract class StatementExecutionFuture<T>
  extends ForwardingListenableFuture.SimpleForwardingListenableFuture<T> implements ListenableFuture<T> {

  protected StatementExecutionFuture(ListenableFuture<T> delegate) {
    super(delegate);
  }

  /**
   * Get the results' schema. This method is there so that we don't have to wait for the whole execution
   * of a query to have access to the schema.
   *
   * @return a list of {@link ColumnDesc} representing the schema of the results. Empty list if there are no results.
   * @throws ExploreException on any error fetching schema.
   * @throws HandleNotFoundException when handle is not found.
   */
  public abstract List<ColumnDesc> getResultSchema() throws ExploreException, HandleNotFoundException;

  /**
   * Get the handle used by the Explore service to represent the execution of the statement.
   */
  public abstract ListenableFuture<Handle> getFutureStatementHandle();

  /**
   * Close the query.
   */
  public abstract void close() throws ExploreException, HandleNotFoundException;

  /**
   * Cancel the query execution.
   */
  public abstract void cancel() throws ExploreException, HandleNotFoundException;
}
