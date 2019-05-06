/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.system;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.ProgramSpecification;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowNodeType;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.metadata.MetadataConstants;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link AbstractSystemMetadataWriter} for a {@link ProgramId program}.
 */
public class ProgramSystemMetadataWriter extends AbstractSystemMetadataWriter {
  private final ProgramId programId;
  private final ProgramSpecification programSpec;
  private final String creationTime;

  public ProgramSystemMetadataWriter(MetadataServiceClient metadataServiceClient, ProgramId programId,
                                     ProgramSpecification programSpec, String creationTime) {
    super(metadataServiceClient, programId);
    this.programId = programId;
    this.programSpec = programSpec;
    this.creationTime = creationTime;
  }

  @Override
  public Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    properties.put(MetadataConstants.ENTITY_NAME_KEY, programId.getEntityName());
    properties.put(VERSION_KEY, programId.getVersion());
    String description = programSpec.getDescription();
    if (!Strings.isNullOrEmpty(description)) {
      properties.put(MetadataConstants.DESCRIPTION_KEY, description);
    }
    properties.put(MetadataConstants.CREATION_TIME_KEY, creationTime);
    return properties.build();
  }

  @Override
  public Set<String> getSystemTagsToAdd() {
    return ImmutableSet.<String>builder()
      .add(programId.getType().getPrettyName())
      .add(getMode())
      .addAll(getWorkflowNodes())
      .build();
  }

  private String getMode() {
    switch (programId.getType()) {
      case MAPREDUCE:
      case SPARK:
      case WORKFLOW:
      case CUSTOM_ACTION:
        return "Batch";
      case WORKER:
      case SERVICE:
        return "Realtime";
      default:
        throw new IllegalArgumentException("Unknown program type " + programId.getType());
    }
  }

  private Iterable<String> getWorkflowNodes() {
    if (ProgramType.WORKFLOW != programId.getType()) {
      return ImmutableSet.of();
    }
    Preconditions.checkArgument(programSpec instanceof WorkflowSpecification,
                                "Expected programSpec %s to be of type WorkflowSpecification", programSpec);
    WorkflowSpecification workflowSpec = (WorkflowSpecification) this.programSpec;
    Set<String> workflowNodeNames = new HashSet<>();
    for (Map.Entry<String, WorkflowNode> entry : workflowSpec.getNodeIdMap().entrySet()) {
      WorkflowNode workflowNode = entry.getValue();
      WorkflowNodeType type = workflowNode.getType();
      // Fork nodes have integers as node ids. Ignore them in system metadata.
      if (WorkflowNodeType.FORK == type) {
        continue;
      }
      workflowNodeNames.add(entry.getKey());
    }
    return workflowNodeNames;
  }
}
