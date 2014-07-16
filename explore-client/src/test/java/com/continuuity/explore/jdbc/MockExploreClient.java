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

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mock Explore client to use in test cases.
 */
public class MockExploreClient implements ExploreClient {

  private final Map<String, List<ColumnDesc>> handlesToMetadata;
  private final Map<String, List<Result>> handlesToResults;
  private final Set<String> fetchedResults;

  public MockExploreClient(Map<String, List<ColumnDesc>> handlesToMetadata,
                           Map<String, List<Result>> handlesToResults) {
    this.handlesToMetadata = Maps.newHashMap(handlesToMetadata);
    this.handlesToResults = Maps.newHashMap(handlesToResults);
    this.fetchedResults = Sets.newHashSet();
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public Handle enableExplore(String datasetInstance) throws ExploreException {
    return null;
  }

  @Override
  public Handle disableExplore(String datasetInstance) throws ExploreException {
    return null;
  }

  @Override
  public Handle execute(String statement) throws ExploreException {
    return Handle.fromId("foobar");
  }

  @Override
  public Status getStatus(Handle handle) throws ExploreException, HandleNotFoundException {
    return new Status(Status.OpStatus.FINISHED, true);
  }

  @Override
  public List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException, HandleNotFoundException {
    if (!handlesToMetadata.containsKey(handle.getHandle())) {
      throw new HandleNotFoundException("Handle not found");
    }
    return handlesToMetadata.get(handle.getHandle());
  }

  @Override
  public List<Result> nextResults(Handle handle, int size) throws ExploreException, HandleNotFoundException {
    // For now we don't consider the size - until needed by other tests

    if (fetchedResults.contains(handle.getHandle())) {
      return Lists.newArrayList();
    }
    if (!handlesToResults.containsKey(handle.getHandle())) {
      throw new HandleNotFoundException("Handle not found");
    }
    fetchedResults.add(handle.getHandle());
    return handlesToResults.get(handle.getHandle());
  }

  @Override
  public void cancel(Handle handle) throws ExploreException, HandleNotFoundException {
    // TODO remove results for given handle
  }

  @Override
  public void close(Handle handle) throws ExploreException, HandleNotFoundException {
    handlesToMetadata.remove(handle.getHandle());
    handlesToResults.remove(handle.getHandle());
  }
}
