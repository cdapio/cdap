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

package co.cask.cdap.etl.proto.v2;

import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline plugin property to a triggered pipeline argument.
 */
public class PluginPropertyMapping extends ArgumentMapping {
  @Nullable
  private final String stageName;

  public PluginPropertyMapping(@Nullable String stageName, @Nullable String source, @Nullable String target) {
    super(source, target);
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
      "stageName='" + getStageName() + '\'' +
      '}';
  }
}
