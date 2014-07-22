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

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * Future object that eventually contains the results of the execution of a statement by Explore.
 * The {@link #get()} method returns an {@link ExploreExecutionResult} object.
 */
public interface StatementExecutionFuture extends ListenableFuture<ExploreExecutionResult> {

  /**
   * Get the results' schema. This method is there so that we don't have to wait for the whole execution
   * of a query to have access to the schema.
   *
   * @return a list of {@link com.continuuity.explore.service.ColumnDesc} representing the schema of the results.
   *         Empty list if there are no results.
   * @throws com.continuuity.explore.service.ExploreException on any error fetching schema.
   */
  public abstract List<ColumnDesc> getResultSchema() throws ExploreException;
}
