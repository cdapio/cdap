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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.List;

/**
 * In-memory implementation of the {@link StreamMetaStore}. Used for testing.
 */
public class InMemoryStreamMetaStore implements StreamMetaStore {

  private final Multimap<String, String> streams;

  public InMemoryStreamMetaStore() {
    this.streams = Multimaps.synchronizedMultimap(HashMultimap.<String, String>create());
  }

  @Override
  public void addStream(String accountId, String streamName) throws Exception {
    streams.put(accountId, streamName);
  }

  @Override
  public void removeStream(String accountId, String streamName) throws Exception {
    streams.remove(accountId, streamName);
  }

  @Override
  public boolean streamExists(String accountId, String streamName) throws Exception {
    return streams.containsEntry(accountId, streamName);
  }

  @Override
  public synchronized List<StreamSpecification> listStreams() throws Exception {
    ImmutableList.Builder<StreamSpecification> builder = ImmutableList.builder();
    for (String stream : streams.values()) {
      builder.add(new StreamSpecification.Builder().setName(stream).create());
    }
    return builder.build();
  }
}
