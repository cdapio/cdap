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

import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;

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
  void unregister(final ApplicationId applicationId);

  Set<DatasetId> getDatasets(final ApplicationId id);

  Set<StreamId> getStreams(final ApplicationId id);

  Set<DatasetId> getDatasets(final ProgramId id);

  Set<StreamId> getStreams(final ProgramId id);

  Set<ProgramId> getPrograms(final StreamId id);

  Set<ProgramId> getPrograms(final DatasetId id);
}
