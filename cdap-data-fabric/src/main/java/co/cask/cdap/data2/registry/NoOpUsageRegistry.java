/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.registry;

import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;

import java.util.Collections;
import java.util.Set;

/**
 * UsageRegistry that does nothing.
 */
public class NoOpUsageRegistry implements UsageRegistry {

  @Override
  public void registerAll(final Iterable<? extends EntityId> users, final StreamId streamId) { }

  @Override
  public void register(EntityId user, StreamId streamId) { }

  @Override
  public void registerAll(final Iterable<? extends EntityId> users, final DatasetId datasetId) { }

  @Override
  public void register(EntityId user, DatasetId datasetId) { }

  @Override
  public void register(final ProgramId programId, final DatasetId datasetInstanceId) { }

  @Override
  public void register(final ProgramId programId, final StreamId streamId) { }

  @Override
  public void unregister(final ApplicationId applicationId) { }

  @Override
  public Set<DatasetId> getDatasets(final ApplicationId id) {
    return Collections.emptySet();
  }

  @Override
  public Set<StreamId> getStreams(final ApplicationId id) {
    return Collections.emptySet();
  }

  @Override
  public Set<DatasetId> getDatasets(final ProgramId id) {
    return Collections.emptySet();
  }

  @Override
  public Set<StreamId> getStreams(final ProgramId id) {
    return Collections.emptySet();
  }

  @Override
  public Set<ProgramId> getPrograms(final StreamId id) {
    return Collections.emptySet();
  }

  @Override
  public Set<ProgramId> getPrograms(final DatasetId id) {
    return Collections.emptySet();
  }
}
