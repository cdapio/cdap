/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.conf;

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRanges;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents metadata about an artifact that can be specified by users.
 */
public class ArtifactConfig  {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeSerializer())
    .create();
  private final Set<ArtifactRange> parents;
  private final Set<PluginClass> plugins;
  private final Map<String, String> properties;

  public ArtifactConfig() {
    this(ImmutableSet.<ArtifactRange>of(), ImmutableSet.<PluginClass>of(), ImmutableMap.<String, String>of());
  }

  public ArtifactConfig(Set<ArtifactRange> parents, Set<PluginClass> plugins, Map<String, String> properties) {
    this.parents = ImmutableSet.copyOf(parents);
    this.plugins = ImmutableSet.copyOf(plugins);
    this.properties = ImmutableMap.copyOf(properties);
  }

  public Set<ArtifactRange> getParents() {
    return parents;
  }

  public Set<PluginClass> getPlugins() {
    return plugins;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean hasParent(Id.Artifact artifactId) {
    for (ArtifactRange range : parents) {
      if (range.getName().equals(artifactId.getName()) && range.versionIsInRange(artifactId.getVersion())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return GSON.toJson(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactConfig that = (ArtifactConfig) o;

    return Objects.equals(parents, that.parents) && Objects.equals(plugins, that.plugins);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parents, plugins);
  }

  /**
   * Serializer for ArtifactRange in a ArtifactConfig. Artifact ranges are expected to be able to be
   * parsed via {@link ArtifactRanges#parseArtifactRange(String)}.
   * Currently, system artifacts can only extend other system artifacts.
   */
  private static final class ArtifactRangeSerializer implements JsonSerializer<ArtifactRange> {

    @Override
    public JsonElement serialize(ArtifactRange src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(src.toString());
    }
  }
}
