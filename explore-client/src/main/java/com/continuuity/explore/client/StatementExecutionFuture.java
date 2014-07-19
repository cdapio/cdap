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

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;

/**
 * Future object that eventually contains the result of the execution of a statement by Explore.
 */
public abstract class StatementExecutionFuture
  extends ForwardingListenableFuture.SimpleForwardingListenableFuture<Iterator<Result>>
  implements ListenableFuture<Iterator<Result>> {

  /**
   * Get the results' schema. This method is there so that we don't have to wait for the whole execution
   * of a query to have access to the schema.
   *
   * @return a list of {@link com.continuuity.explore.service.ColumnDesc} representing the schema of the results.
   *         Empty list if there are no results.
   * @throws com.continuuity.explore.service.ExploreException on any error fetching schema.
   * @throws com.continuuity.explore.service.HandleNotFoundException when handle is not found.
   */
  public abstract List<ColumnDesc> getResultSchema() throws ExploreException, HandleNotFoundException;

  /**
   * Get the handle used by the Explore service to execute the statement.
   */
  public abstract ListenableFuture<Handle> getFutureStatementHandle();

  /**
   * Close the query.
   */
  public abstract void close() throws ExploreException, HandleNotFoundException;

  /**
   * Cancel the query execution. This method does not call cancel on the future object.
   */
  public abstract void cancel() throws ExploreException, HandleNotFoundException;

  protected StatementExecutionFuture(ListenableFuture<Iterator<Result>> delegate) {
    super(delegate);
  }
}
