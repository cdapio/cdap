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

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.ViewSpecification;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * A {@link StreamAdmin} that does nothing.
 */
public class NoopStreamAdmin implements StreamAdmin {

  @Override
  public void dropAllInNamespace(Id.Namespace namespace) throws Exception {
  }

  @Override
  public void configureInstances(Id.Stream streamId, long groupId, int instances) throws Exception {
  }

  @Override
  public void configureGroups(Id.Stream streamId, Map<Long, Integer> groupInfo) throws Exception {
  }

  @Override
  public void upgrade() throws Exception {
  }

  @Override
  public StreamConfig getConfig(Id.Stream streamId) throws IOException {
    throw new IllegalStateException("Stream " + streamId + " not exists.");
  }

  @Override
  public void updateConfig(Id.Stream streamId, StreamProperties properties) throws IOException {
  }

  @Override
  public boolean exists(Id.Stream streamId) throws Exception {
    return false;
  }

  @Override
  public StreamConfig create(Id.Stream streamId) throws Exception {
    return null;
  }

  @Override
  public StreamConfig create(Id.Stream streamId, @Nullable Properties props) throws Exception {
    return null;
  }

  @Override
  public void truncate(Id.Stream streamId) throws Exception {
  }

  @Override
  public void drop(Id.Stream streamId) throws Exception {
  }

  @Override
  public boolean createOrUpdateView(Id.Stream.View viewId, ViewSpecification spec) throws Exception {
    return false;
  }

  @Override
  public void deleteView(Id.Stream.View viewId) throws Exception {

  }

  @Override
  public List<Id.Stream.View> listViews(Id.Stream streamId) {
    return null;
  }

  @Override
  public ViewSpecification getView(Id.Stream.View viewId) {
    return null;
  }

  @Override
  public boolean viewExists(Id.Stream.View viewId) throws Exception {
    return false;
  }

  @Override
  public void register(Iterable<? extends Id> owners, Id.Stream streamId) {
  }

  @Override
  public void addAccess(Id.Run run, Id.Stream streamId, AccessType accessType) {
  }
}
