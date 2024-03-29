/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.registry;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.ProgramId;
import java.util.Set;

/**
 * Store program -> dataset usage information.
 */
public interface UsageRegistry extends UsageWriter {

  /**
   * Unregisters all usage information of an application.
   *
   * @param applicationId application
   */
  void unregister(ApplicationId applicationId);

  Set<DatasetId> getDatasets(ApplicationId id);

  Set<DatasetId> getDatasets(ProgramId id);

  Set<ProgramId> getPrograms(DatasetId id);
}
