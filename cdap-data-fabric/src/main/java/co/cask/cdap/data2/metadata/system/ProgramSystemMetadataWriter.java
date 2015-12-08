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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A {@link AbstractSystemMetadataWriter} for a {@link Id.Program program}.
 */
public class ProgramSystemMetadataWriter extends AbstractSystemMetadataWriter {
  private final Id.Program programId;
  private final ProgramSpecification programSpec;

  public ProgramSystemMetadataWriter(MetadataStore metadataStore, Id.Program programId,
                                     ProgramSpecification programSpec) {
    super(metadataStore, programId);
    this.programId = programId;
    this.programSpec = programSpec;
  }

  @Override
  Map<String, String> getSystemPropertiesToAdd() {
    return ImmutableMap.of();
  }

  @Override
  String[] getSystemTagsToAdd() {
    return new String[]{programId.getType().getPrettyName(), getMode()};
  }

  public String getMode() {
    String mode;
    switch (programId.getType()) {
      case MAPREDUCE:
      case SPARK:
      case WORKFLOW:
      case CUSTOM_ACTION:
        mode = "Batch";
        break;
      case FLOW:
      case WORKER:
      case SERVICE:
        mode = "Real-time";
        break;
      default:
        throw new IllegalArgumentException("Unknown program type " + programId.getType());
    }
    return mode;
  }
}
