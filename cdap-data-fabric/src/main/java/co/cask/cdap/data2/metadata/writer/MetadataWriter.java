/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.writer;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.ProgramRunId;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Record metadata for entities.
 */
public interface MetadataWriter {

  /**
   * Add metadata for an entity.
   */
  void add(ProgramRunId run, MetadataEntity entity,
           @Nullable Map<String, String> propertiesToAdd,
           @Nullable Set<String> tagsToAdd);

  /**
   * Delete metadata for an entity.
   */
  void remove(ProgramRunId run, MetadataEntity entity,
              @Nullable Set<String> propertiesToDelete,
              @Nullable Set<String> tagsToDelete);
}
