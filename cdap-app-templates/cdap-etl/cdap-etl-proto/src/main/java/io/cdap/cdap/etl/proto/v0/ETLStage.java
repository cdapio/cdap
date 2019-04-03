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

package co.cask.cdap.etl.proto.v0;

import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.UpgradeContext;
import co.cask.cdap.etl.proto.v1.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link ETLStage} as it existed in 3.2.x. The name and properties in this object were moved to the
 * {@link Plugin} class.
 */
public class ETLStage {
  private final String name;
  private final Map<String, String> properties;
  private final String errorDatasetName;

  public ETLStage(String name, Map<String, String> properties, @Nullable String errorDatasetName) {
    this.name = name;
    this.properties = properties;
    this.errorDatasetName = errorDatasetName;
  }

  public String getName() {
    return name;
  }

  public String getErrorDatasetName() {
    return errorDatasetName;
  }

  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties == null ? new HashMap<String, String>() : properties);
  }

  co.cask.cdap.etl.proto.v1.ETLStage upgradeStage(String name, String pluginType, UpgradeContext upgradeContext) {
    ArtifactSelectorConfig artifactSelectorConfig = upgradeContext.getPluginArtifact(pluginType, this.name);
    Plugin plugin = new Plugin(this.name, getProperties(), artifactSelectorConfig);
    return new co.cask.cdap.etl.proto.v1.ETLStage(name, plugin, this.errorDatasetName);
  }
}
