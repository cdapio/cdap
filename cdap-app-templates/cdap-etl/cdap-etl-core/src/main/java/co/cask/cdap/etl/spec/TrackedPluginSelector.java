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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginSelector;

import java.util.Map;
import java.util.SortedMap;

/**
 * Wrapper around {@link PluginSelector} that delegates to another selector but tracks which artifact it chose.
 */
public class TrackedPluginSelector extends PluginSelector {
  private final PluginSelector delegate;
  private ArtifactId selectedArtifact;

  public TrackedPluginSelector(PluginSelector delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map.Entry<ArtifactId, PluginClass> select(SortedMap<ArtifactId, PluginClass> plugins) {
    Map.Entry<ArtifactId, PluginClass> selected = delegate.select(plugins);
    selectedArtifact = selected.getKey();
    return selected;
  }

  public ArtifactId getSelectedArtifact() {
    return selectedArtifact;
  }
}
