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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The mapping between triggering pipeline properties to the triggered pipeline arguments.
 */
public class TriggeringPipelinePropertyMapping {
  private final List<ArgumentMapping> arguments;
  private final List<PluginPropertyMapping> pluginProperties;

  public TriggeringPipelinePropertyMapping() {
    this.arguments = new ArrayList<>();
    this.pluginProperties = new ArrayList<>();
  }

  /**
   * @return The list of mapping between triggering pipeline arguments to triggered pipeline arguments
   */
  public List<ArgumentMapping> getArguments() {
    return arguments;
  }

  /**
   * @return The list of mapping between triggering pipeline plugin properties to triggered pipeline arguments
   */
  public List<PluginPropertyMapping> getPluginProperties() {
    return pluginProperties;
  }

  @VisibleForTesting
  public void addArgumentMapping(ArgumentMapping argumentMapping) {
    arguments.add(argumentMapping);
  }

  @VisibleForTesting
  public void addPluginPropertyMapping(PluginPropertyMapping pluginPropertyMapping) {
    pluginProperties.add(pluginPropertyMapping);
  }

  /**
   * The mapping between a triggering pipeline argument to a triggered pipeline argument.
   */
  public static class ArgumentMapping {
    private final String source;
    @Nullable
    private final String target;

    public ArgumentMapping(String source, @Nullable String target) {
      this.source = source;
      this.target = target;
    }

    /**
     * @return The name of triggering pipeline argument
     */
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
  }

  /**
   * The mapping between a triggering pipeline plugin property to a triggered pipeline argument.
   */
  public static class PluginPropertyMapping extends ArgumentMapping {
    private final String stageName;

    public PluginPropertyMapping(String stageName, String source, @Nullable String target) {
      super(source, target);
      this.stageName = stageName;
    }

    /**
     * @return The name of the stage where the triggering pipeline plugin property is defined
     */
    public String getStageName() {
      return stageName;
    }
  }
}
