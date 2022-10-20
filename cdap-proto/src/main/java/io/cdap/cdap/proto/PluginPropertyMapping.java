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
import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline plugin property to a triggered pipeline argument.
 */
public class PluginPropertyMapping extends ArgumentMapping {
  @Nullable
  private final String stageName;

  public PluginPropertyMapping(@Nullable String stageName, @Nullable String source, @Nullable String target) {
    this(stageName, source, target, null);
  }

  public PluginPropertyMapping(@Nullable String stageName,
                               @Nullable String source,
                               @Nullable String target,
                               @Nullable TriggeringPipelineId pipelineId) {
    super(source, target, pipelineId);
    this.stageName = stageName;
  }

  /**
   * @return The name of the stage where the triggering pipeline plugin property is defined
   */
  @Nullable
  public String getStageName() {
    return stageName;
  }

  @Override
  public String toString() {
    return "PluginPropertyMapping{" +
      "source='" + getSource() + '\'' +
      ", target='" + getTarget() + '\'' +
      ", stageName='" + getStageName() + '\'' +
      ", triggeringPipelineId='" + getTriggeringPipelineId() + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PluginPropertyMapping)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    PluginPropertyMapping that = (PluginPropertyMapping) o;
    return Objects.equals(getStageName(), that.getStageName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getStageName());
  }
}
