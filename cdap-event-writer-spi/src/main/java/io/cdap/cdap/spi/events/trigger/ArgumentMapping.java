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

package io.cdap.cdap.spi.events.trigger;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline argument to a triggered pipeline argument.
 */
public class ArgumentMapping {
  @Nullable
  private final String source;
  @Nullable
  private final String target;
  @Nullable
  private final String pipelineNamespace;
  @Nullable
  private final String pipelineName;

  public ArgumentMapping(@Nullable String source,
                         @Nullable String target,
                         @Nullable String pipelineNamespace,
                         @Nullable String pipelineName) {
    this.source = source;
    this.target = target;
    this.pipelineNamespace = pipelineNamespace;
    this.pipelineName = pipelineName;
  }

  /**
   * @return The name of triggering pipeline argument
   */
  @Nullable
  public String getSource() {
    return source;
  }

  /**
   * @return The name of triggered pipeline argument
   */
  @Nullable
  public String getTarget() {
    return target;
  }

  /**
   * @return The namespace of the triggering pipeline
   */
  @Nullable
  public String getPipelineNamespace() {
    return pipelineNamespace;
  }

  /**
   * @return The name of the triggering pipeline
   */
  @Nullable
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public String toString() {
    return "ArgumentMapping{" +
      "source='" + source + '\'' +
      ", target='" + target + '\'' +
      ", pipelineNamespace='" + pipelineNamespace + '\'' +
      ", pipelineName='" + pipelineName + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArgumentMapping)) {
      return false;
    }
    ArgumentMapping that = (ArgumentMapping) o;
    return Objects.equals(getSource(), that.getSource())
      && Objects.equals(getTarget(), that.getTarget())
      && Objects.equals(getPipelineNamespace(), that.getPipelineNamespace())
      && Objects.equals(getPipelineName(), that.getPipelineName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSource(), getTarget(), getPipelineNamespace(), getPipelineName());
  }
}
