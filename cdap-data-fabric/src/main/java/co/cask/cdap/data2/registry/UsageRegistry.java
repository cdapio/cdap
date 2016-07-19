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

package co.cask.cdap.data2.registry;

import co.cask.cdap.proto.Id;

import java.util.Set;

/**
 * Store program -> dataset/stream usage information.
 */
public interface UsageRegistry extends RuntimeUsageRegistry {

  /**
   * Unregisters all usage information of an application.
   *
   * @param applicationId application
   */
  void unregister(final Id.Application applicationId);

  Set<Id.DatasetInstance> getDatasets(final Id.Application id);

  Set<Id.Stream> getStreams(final Id.Application id);

  Set<Id.DatasetInstance> getDatasets(final Id.Program id);

  Set<Id.Stream> getStreams(final Id.Program id);

  Set<Id.Program> getPrograms(final Id.Stream id);

  Set<Id.Program> getPrograms(final Id.DatasetInstance id);
}
