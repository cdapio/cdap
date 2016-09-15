/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * In-memory implementation of the {@link StreamMetaStore}. Used for testing.
 */
public class InMemoryStreamMetaStore implements StreamMetaStore {

  private final Multimap<String, String> streams;

  public InMemoryStreamMetaStore() {
    this.streams = Multimaps.synchronizedMultimap(HashMultimap.<String, String>create());
  }

  @Override
  public void addStream(StreamId streamId) throws Exception {
    streams.put(streamId.getNamespace(), streamId.getEntityName());
  }

  @Override
  public void addStream(StreamId streamId, @Nullable String description) throws Exception {
    addStream(streamId);
  }

  @Override
  public StreamSpecification getStream(StreamId streamId) throws Exception {
    if (!streamExists(streamId)) {
      return null;
    }
    return new StreamSpecification.Builder().setName(streamId.getEntityName()).create();
  }

  @Override
  public void removeStream(StreamId streamId) throws Exception {
    streams.remove(streamId.getNamespace(), streamId.getEntityName());
  }

  @Override
  public boolean streamExists(StreamId streamId) throws Exception {
    return streams.containsEntry(streamId.getNamespace(), streamId.getEntityName());
  }

  @Override
  public List<StreamSpecification> listStreams(NamespaceId namespaceId) throws Exception {
    ImmutableList.Builder<StreamSpecification> builder = ImmutableList.builder();
    synchronized (streams) {
      for (String stream : streams.get(namespaceId.getEntityName())) {
        builder.add(new StreamSpecification.Builder().setName(stream).create());
      }
    }
    return builder.build();
  }

  @Override
  public synchronized Multimap<NamespaceId, StreamSpecification> listStreams() throws Exception {
    ImmutableMultimap.Builder<NamespaceId, StreamSpecification> builder = ImmutableMultimap.builder();
    for (String namespaceId : streams.keySet()) {
      synchronized (streams) {
        Collection<String> streamNames = streams.get(namespaceId);
        builder.putAll(new NamespaceId(namespaceId),
                       Collections2.transform(streamNames, new Function<String, StreamSpecification>() {
                         @Nullable
                         @Override
                         public StreamSpecification apply(String input) {
                           return new StreamSpecification.Builder().setName(input).create();
                         }
                       }));
      }
    }
    return builder.build();
  }
}
