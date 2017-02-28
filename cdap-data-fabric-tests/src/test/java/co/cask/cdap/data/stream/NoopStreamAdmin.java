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

package co.cask.cdap.data.stream;

import co.cask.cdap.api.data.stream.StreamProperties;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link StreamAdmin} that does nothing.
 */
public class NoopStreamAdmin implements StreamAdmin {

  @Override
  public void dropAllInNamespace(NamespaceId namespace) throws Exception {
  }

  @Override
  public void configureInstances(StreamId streamId, long groupId, int instances) throws Exception {
  }

  @Override
  public void configureGroups(StreamId streamId, Map<Long, Integer> groupInfo) throws Exception {
  }

  @Override
  public void upgrade() throws Exception {
  }

  @Override
  public List<StreamSpecification> listStreams(NamespaceId namespaceId) throws Exception {
    return ImmutableList.of();
  }

  @Override
  public StreamConfig getConfig(StreamId streamId) throws IOException {
    throw new IllegalStateException("Stream " + streamId + " not exists.");
  }

  @Override
  public StreamProperties getProperties(StreamId streamId) throws IOException {
    throw new IllegalStateException("Stream " + streamId + " not exists.");
  }

  @Override
  public void updateConfig(StreamId streamId, StreamProperties properties) throws IOException {
  }

  @Override
  public boolean exists(StreamId streamId) throws Exception {
    return false;
  }

  @Override
  @Nullable
  public StreamConfig create(StreamId streamId) throws Exception {
    return null;
  }

  @Override
  @Nullable
  public StreamConfig create(StreamId streamId, @Nullable StreamProperties props) throws Exception {
    return null;
  }

  @Override
  public void truncate(StreamId streamId) throws Exception {
  }

  @Override
  public void drop(StreamId streamId) throws Exception {
  }

  @Override
  public boolean createOrUpdateView(StreamViewId viewId, ViewSpecification spec) throws Exception {
    return false;
  }

  @Override
  public void deleteView(StreamViewId viewId) throws Exception {

  }

  @Override
  public List<StreamViewId> listViews(StreamId streamId) {
    return null;
  }

  @Override
  public ViewSpecification getView(StreamViewId viewId) {
    return null;
  }

  @Override
  public boolean viewExists(StreamViewId viewId) throws Exception {
    return false;
  }

  @Override
  public void register(Iterable<? extends EntityId> owners, StreamId streamId) {
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId streamId, AccessType accessType) {
  }
}
