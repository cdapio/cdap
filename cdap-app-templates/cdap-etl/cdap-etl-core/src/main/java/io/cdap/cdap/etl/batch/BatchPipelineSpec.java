/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationUpgradeContext;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.v2.spec.PipelineSpec;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

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

  public List<ActionSpec> upgradeActionSpecs(ApplicationUpgradeContext upgradeContext) {
    // Jay Pandya: Check if this is needed.
    return getEndingActions();
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

    public Builder addActions(List<ActionSpec> actions) {
      this.endingActions.addAll(actions);
      return this;
    }

    @Override
    public BatchPipelineSpec build() {
      return new BatchPipelineSpec(stages, connections, resources, driverResources, clientResources,
                                   stageLoggingEnabled, processTimingEnabled, endingActions,
                                   numOfRecordsPreview, properties);
    }
  }
}
