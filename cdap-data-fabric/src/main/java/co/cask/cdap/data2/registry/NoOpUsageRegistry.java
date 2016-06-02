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

import co.cask.cdap.proto.Id;

import java.util.Collections;
import java.util.Set;

/**
 * UsageRegistry that does nothing.
 */
public class NoOpUsageRegistry implements UsageRegistry {

  @Override
  public void registerAll(final Iterable<? extends Id> users, final Id.Stream streamId) { }

  @Override
  public void register(Id user, Id.Stream streamId) { }

  @Override
  public void registerAll(final Iterable<? extends Id> users, final Id.DatasetInstance datasetId) { }

  @Override
  public void register(Id user, Id.DatasetInstance datasetId) { }

  @Override
  public void register(final Id.Program programId, final Id.DatasetInstance datasetInstanceId) { }

  @Override
  public void register(final Id.Program programId, final Id.Stream streamId) { }

  @Override
  public void unregister(final Id.Application applicationId) { }

  @Override
  public Set<Id.DatasetInstance> getDatasets(final Id.Application id) {
    return Collections.emptySet();
  }

  @Override
  public Set<Id.Stream> getStreams(final Id.Application id) {
    return Collections.emptySet();
  }

  @Override
  public Set<Id.DatasetInstance> getDatasets(final Id.Program id) {
    return Collections.emptySet();
  }

  @Override
  public Set<Id.Stream> getStreams(final Id.Program id) {
    return Collections.emptySet();
  }

  @Override
  public Set<Id.Program> getPrograms(final Id.Stream id) {
    return Collections.emptySet();
  }

  @Override
  public Set<Id.Program> getPrograms(final Id.DatasetInstance id) {
    return Collections.emptySet();
  }
}
