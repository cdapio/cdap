/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data.stream.preview;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 *  StreamAdmin to be used for preview. Any operation that modifies the stream will be no-op.
 */
public class PreviewStreamAdmin implements StreamAdmin {

  private final StreamAdmin delegate;

  public PreviewStreamAdmin(StreamAdmin delegate) {
    this.delegate = delegate;
  }

  @Override
  public void dropAllInNamespace(NamespaceId namespace) throws Exception {
    // no-op
  }

  @Override
  public void configureInstances(StreamId streamId, long groupId, int instances) throws Exception {
    // no-op
  }

  @Override
  public void configureGroups(StreamId streamId, Map<Long, Integer> groupInfo) throws Exception {
    // no-op
  }

  @Override
  public void upgrade() throws Exception {
    // no-op
  }

  @Override
  public List<StreamSpecification> listStreams(NamespaceId namespaceId) throws Exception {
    return delegate.listStreams(namespaceId);
  }

  @Override
  public StreamConfig getConfig(StreamId streamId) throws IOException {
    return delegate.getConfig(streamId);
  }

  @Override
  public StreamProperties getProperties(StreamId streamId) throws Exception {
    return delegate.getProperties(streamId);
  }

  @Override
  public void updateConfig(StreamId streamId, StreamProperties properties) throws Exception {
    // no-op
  }

  @Override
  public boolean exists(StreamId streamId) throws Exception {
    return delegate.exists(streamId);
  }

  @Nullable
  @Override
  public StreamConfig create(StreamId streamId) throws Exception {
    return null;
  }

  @Nullable
  @Override
  public StreamConfig create(StreamId streamId, @Nullable Properties props) throws Exception {
    return null;
  }

  @Override
  public void truncate(StreamId streamId) throws Exception {
    // no-op
  }

  @Override
  public void drop(StreamId streamId) throws Exception {
    // no-op
  }

  @Override
  public boolean createOrUpdateView(StreamViewId viewId, ViewSpecification spec) throws Exception {
    return false;
  }

  @Override
  public void deleteView(StreamViewId viewId) throws Exception {
    // no-op
  }

  @Override
  public List<StreamViewId> listViews(StreamId streamId) throws Exception {
    return delegate.listViews(streamId);
  }

  @Override
  public ViewSpecification getView(StreamViewId viewId) throws Exception {
    return delegate.getView(viewId);
  }

  @Override
  public boolean viewExists(StreamViewId viewId) throws Exception {
    return delegate.viewExists(viewId);
  }

  @Override
  public void register(Iterable<? extends EntityId> owners, StreamId streamId) {
    // no-op
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId streamId, AccessType accessType) {
    // no-op
  }
}
