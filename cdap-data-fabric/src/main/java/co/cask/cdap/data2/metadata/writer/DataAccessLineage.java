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

import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Class for carrying data access lineage information emitted from program execution.
 */
public class DataAccessLineage {

  private static final Logger LOG = LoggerFactory.getLogger(DataAccessLineage.class);

  private final long accessTime;
  private final ProgramRunId programRunId;
  private final AccessType accessType;
  @Nullable
  private final DatasetId datasetId;

  // We don't serialize this field, but instead serialize the class name and the id parts
  @Nullable
  private transient NamespacedEntityId componentId;
  @Nullable
  private final String componentIdClassName;
  private final List<String> componentIdParts;

  // Stream is deprecated since CDAP 5. Expect the field will be removed in future.
  @Nullable
  private final StreamId streamId;

  public DataAccessLineage(ProgramRunId programRunId, AccessType accessType, EntityId dataEntityId,
                           @Nullable NamespacedEntityId componentId) {
    this(programRunId, accessType, dataEntityId, componentId, System.currentTimeMillis());
  }

  @VisibleForTesting
  DataAccessLineage(ProgramRunId programRunId, AccessType accessType, EntityId dataEntityId,
                    @Nullable NamespacedEntityId componentId, long accessTime) {
    if (dataEntityId instanceof DatasetId) {
      this.datasetId = (DatasetId) dataEntityId;
      this.streamId = null;
    } else if (dataEntityId instanceof StreamId) {
      this.datasetId = null;
      this.streamId = (StreamId) dataEntityId;
    } else {
      // This should never happen
      throw new IllegalArgumentException("Only instance of DatasetId or StreamId can be used as the dataEntityId");
    }

    this.accessTime = accessTime;
    this.programRunId = programRunId;
    this.accessType = accessType;
    this.componentId = componentId;

    if (componentId != null) {
      componentIdClassName = componentId.getClass().getName();
      componentIdParts = StreamSupport.stream(componentId.toIdParts().spliterator(), false)
                                      .collect(Collectors.toList());
    } else {
      componentIdClassName = null;
      componentIdParts = null;
    }
  }

  public long getAccessTime() {
    return accessTime;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public AccessType getAccessType() {
    return accessType;
  }

  @Nullable
  public DatasetId getDatasetId() {
    return datasetId;
  }

  @Nullable
  public NamespacedEntityId getComponentId() {
    if (componentId != null) {
      return componentId;
    }
    if (componentIdClassName == null || componentIdParts == null) {
      return null;
    }

    try {
      // Use reflection to invoke the fromIdParts method, which should be available for all EntityId classes.
      componentId = (NamespacedEntityId) Class.forName(componentIdClassName)
        .getMethod("fromIdParts", Iterable.class).invoke(null, componentIdParts);
      return componentId;
    } catch (Exception e) {
      // If there is any failure (which shouldn't), treat it as no component id.
      LOG.debug("Failed to construct component Id for class {} from id parts {}",
                componentIdClassName, componentIdParts);
      return null;
    }
  }

  @Nullable
  public StreamId getStreamId() {
    return streamId;
  }

  @Override
  public String toString() {
    return "DataAccessLineage{" +
      "accessTime=" + accessTime +
      ", programRunId=" + programRunId +
      ", accessType=" + accessType +
      ", datasetId=" + datasetId +
      ", componentId=" + getComponentId() +
      ", streamId=" + streamId +
      '}';
  }
}
