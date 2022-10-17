/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import java.util.Objects;

/**
 * Class for identifiers of properties from the triggering pipeline.
 */
public class TriggeringPipelineId {
  private final String namespace;
  private final String pipelineName;

  public TriggeringPipelineId(String namespace, String pipelineName) {
    this.namespace = namespace;
    this.pipelineName = pipelineName;
  }

  /**
   * @return Namespace of the triggering pipeline.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return Names of the triggering pipeline.
   */
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public String toString() {
    return "TriggeringPipelineId{" +
      "namespace='" + getNamespace() + '\'' +
      ", pipelineName='" + getPipelineName() + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TriggeringPipelineId)) {
      return false;
    }
    TriggeringPipelineId that = (TriggeringPipelineId) o;
    return getNamespace().equals(that.getNamespace())
      && getPipelineName().equals(that.getPipelineName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNamespace(), getPipelineName());
  }
}
