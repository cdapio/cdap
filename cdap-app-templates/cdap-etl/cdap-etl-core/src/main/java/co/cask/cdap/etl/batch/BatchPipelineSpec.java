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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.spec.PipelineSpec;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Specification for a batch pipeline.
 */
public class BatchPipelineSpec extends PipelineSpec {
  private final List<ActionSpec> endingActions;

  private BatchPipelineSpec(Set<StageSpec> stages,
                            Set<Connection> connections,
                            Resources resources,
                            Resources driverResources,
                            Resources clientResources,
                            boolean stageLoggingEnabled,
                            boolean processTimingEnabled,
                            List<ActionSpec> endingActions,
                            int numOfRecordsPreview,
                            Map<String, String> properties) {
    super(stages, connections, resources, driverResources, clientResources, stageLoggingEnabled, processTimingEnabled,
          numOfRecordsPreview, properties);
    this.endingActions = ImmutableList.copyOf(endingActions);
  }

  public List<ActionSpec> getEndingActions() {
    return endingActions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    BatchPipelineSpec that = (BatchPipelineSpec) o;

    return Objects.equals(endingActions, that.endingActions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), endingActions);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating a BatchPipelineSpec.
   */
  public static class Builder extends PipelineSpec.Builder<Builder> {
    private List<ActionSpec> endingActions;

    public Builder() {
      this.endingActions = new ArrayList<>();
    }

    public Builder addAction(ActionSpec action) {
      endingActions.add(action);
      return this;
    }

    public BatchPipelineSpec build() {
      return new BatchPipelineSpec(stages, connections, resources, driverResources, clientResources,
                                   stageLoggingEnabled, processTimingEnabled, endingActions,
                                   numOfRecordsPreview, properties);
    }
  }
}
