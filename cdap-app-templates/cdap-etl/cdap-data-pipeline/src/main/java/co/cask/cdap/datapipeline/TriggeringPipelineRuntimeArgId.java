/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import java.util.Objects;

/**
 * Identifier of runtime arguments from the triggering pipeline.
 */
public class TriggeringPipelineRuntimeArgId extends TriggeringPipelinePropertyId {
  private final String runtimeArgumentKey;

  public TriggeringPipelineRuntimeArgId(String namespace, String pipelineName, String runtimeArgumentKey) {
    super(Type.RUNTIME_ARG, namespace, pipelineName);
    this.runtimeArgumentKey = runtimeArgumentKey;
  }

  /**
   * @return The key of the runtime argument in the triggering pipeline.
   */
  public String getRuntimeArgumentKey() {
    return runtimeArgumentKey;
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

    TriggeringPipelineRuntimeArgId that = (TriggeringPipelineRuntimeArgId) o;
    return Objects.equals(getRuntimeArgumentKey(), that.getRuntimeArgumentKey());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getRuntimeArgumentKey());
  }
}
