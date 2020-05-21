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

package io.cdap.cdap.etl.spec;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.api.app.ArtifactSelectorConfig;

import java.util.Map;
import java.util.SortedMap;
import javax.annotation.Nullable;

/**
 * Wrapper around {@link PluginSelector} that delegates to another selector but tracks which artifact it chose.
 */
public class TrackedPluginSelector extends PluginSelector {
  private final PluginSelector delegate;
  private ArtifactId selectedArtifact;
  private ArtifactSelectorConfig suggestion;

  public TrackedPluginSelector(PluginSelector delegate) {
    this.delegate = delegate;
  }

  @Nullable
  @Override
  public Map.Entry<ArtifactId, PluginClass> select(SortedMap<ArtifactId, PluginClass> plugins) {
    ArtifactId latestArtifact = plugins.tailMap(plugins.lastKey()).entrySet().iterator().next().getKey();
    suggestion = new ArtifactSelectorConfig(latestArtifact.getScope().name(), latestArtifact.getName(),
                                            latestArtifact.getVersion().getVersion());

    Map.Entry<ArtifactId, PluginClass> selected = delegate.select(plugins);
    selectedArtifact = selected == null ? null : selected.getKey();
    return selected;
  }

  public ArtifactId getSelectedArtifact() {
    return selectedArtifact;
  }

  /**
   * @return a suggested artifact if none was selected
   */
  public ArtifactSelectorConfig getSuggestion() {
    return suggestion;
  }
}
