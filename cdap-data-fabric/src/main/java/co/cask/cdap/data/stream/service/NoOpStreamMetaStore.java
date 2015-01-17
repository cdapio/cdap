/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * No-op implementation of the {@link StreamMetaStore}. Used for testing.
 */
public class NoOpStreamMetaStore implements StreamMetaStore {

  @Override
  public void addStream(String accountId, String streamName) throws Exception {
    // No-op
  }

  @Override
  public void removeStream(String accountId, String streamName) throws Exception {
    // No-op
  }

  @Override
  public boolean streamExists(String accountId, String streamName) throws Exception {
    return true;
  }

  @Override
  public List<StreamSpecification> listStreams() throws Exception {
    return ImmutableList.of();
  }
}
