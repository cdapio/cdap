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

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.InvalidArtifactRangeException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a system artifact along with metadata about that artifact. The metadata can come from a json config file
 * that is placed in the system artifacts directory. Metadata includes other artifacts that this artifact extends,
 * as well as plugin information for the artifact. It is comparable so that we
 * can order collections of configs such that artifacts that extends other artifacts are later in the collection.
 */
public class ArtifactConfig implements Comparable<ArtifactConfig> {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeDeserializer())
    .registerTypeAdapter(ArtifactConfig.class, new SystemArtifactConfigDeserializer())
    .create();
  private final Set<ArtifactRange> parents;
  private final Set<PluginClass> plugins;
  private File artifactFile;
  private Id.Artifact artifactId;

  private ArtifactConfig(Set<ArtifactRange> parents, Set<PluginClass> plugins) {
    this(null, null, parents, plugins);
  }

  private ArtifactConfig(Id.Artifact artifactId, File artifactFile,
                         Set<ArtifactRange> parents, Set<PluginClass> plugins) {
    this.artifactId = artifactId;
    this.artifactFile = artifactFile;
    this.parents = parents;
    this.plugins = plugins;
  }

  public Id.Artifact getArtifactId() {
    return artifactId;
  }

  public File getFile() {
    return artifactFile;
  }

  public Set<ArtifactRange> getParents() {
    return parents;
  }

  public Set<PluginClass> getPlugins() {
    return plugins;
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
  public int compareTo(ArtifactConfig that) {
    // assumes there are no circular dependencies, since extensions should be at most 1 level deep.
    // that is, an artifact cannot have as a parent another artifact that has parents.
    boolean thisIsParentOfThat = that.hasParent(artifactId);
    boolean thatIsParentOfThis = hasParent(that.artifactId);
    if (thisIsParentOfThat) {
      return -1;
    }
    if (thatIsParentOfThis) {
      return 1;
    }
    return artifactId.compareTo(that.artifactId);
  }

  /**
   * Read the contents of the given file and deserialize it into an ArtifactConfig.
   *
   * @param artifactId the id of the artifact this config file is for
   * @param configFile the config file to read
   * @return the contents of the file parsed as an ArtifactConfig
   * @throws IOException if there was a problem reading the file, for example if the file does not exist.
   * @throws InvalidArtifactException if there was a problem deserializing the file contents due to some invalid
   *                                  format or unexpected value.
   */
  public static ArtifactConfig read(Id.Artifact artifactId,
                                          File configFile,
                                          File artifactFile) throws IOException, InvalidArtifactException {
    String fileName = configFile.getName();
    try (Reader reader = Files.newReader(configFile, Charsets.UTF_8)) {
      try {
        ArtifactConfig config = GSON.fromJson(reader, ArtifactConfig.class);
        config.artifactId = artifactId;
        config.artifactFile = artifactFile;
        return config;
      } catch (JsonSyntaxException e) {
        throw new InvalidArtifactException(String.format("%s contains invalid json: %s", fileName, e.getMessage()), e);
      } catch (JsonParseException e) {
        throw new InvalidArtifactException(String.format("Unable to parse %s: %s", fileName, e.getMessage()), e);
      }
    }
  }

  /**
   * Get a builder to create an artifact config for a given artifact id.
   *
   * @param artifactId the id of the artifact to create a config for
   * @return a builder for creating an artifact config
   */
  public static Builder builder(Id.Artifact artifactId, File artifactFile) {
    return new Builder(artifactId, artifactFile);
  }

  /**
   * Builder for creating a ArtifactConfig.
   */
  public static class Builder {
    private final Id.Artifact artifactId;
    private final File artifactFile;
    private final Set<PluginClass> plugins;
    private final Set<ArtifactRange> parents;

    private Builder(Id.Artifact artifactId, File artifactFile) {
      this.artifactId = artifactId;
      this.artifactFile = artifactFile;
      this.plugins = new HashSet<>();
      this.parents = new HashSet<>();
    }

    public Builder addPlugins(PluginClass pluginClass, PluginClass... plugins) {
      this.plugins.add(pluginClass);
      Collections.addAll(this.plugins, plugins);
      return this;
    }

    public Builder addPlugins(Collection<PluginClass> plugins) {
      this.plugins.addAll(plugins);
      return this;
    }

    public Builder addParents(ArtifactRange parent, ArtifactRange... parents) {
      this.parents.add(parent);
      Collections.addAll(this.parents, parents);
      return this;
    }

    public ArtifactConfig build() {
      return new ArtifactConfig(artifactId, artifactFile,
        Collections.unmodifiableSet(parents), Collections.unmodifiableSet(plugins));
    }
  }

  /**
   * Deserializer for ArtifactRange in a ArtifactConfig. Artifact ranges are expected to be able to be
   * parsed via {@link ArtifactRange#parse(String)}. Currently, system artifacts can only extend other system artifacts.
   */
  private static final class ArtifactRangeDeserializer
    implements JsonDeserializer<ArtifactRange>, JsonSerializer<ArtifactRange> {

    @Override
    public ArtifactRange deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {
      if (!json.isJsonPrimitive()) {
        throw new JsonParseException("Parent artifacts should be strings.");
      }

      try {
        return ArtifactRange.parse(Id.Namespace.SYSTEM, json.getAsString());
      } catch (InvalidArtifactRangeException e) {
        throw new JsonParseException(e.getMessage(), e);
      }
    }

    @Override
    public JsonElement serialize(ArtifactRange src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(src.toNonNamespacedString());
    }
  }

  /**
   * Serializer and Deserializer for ArtifactConfig. Used to make sure collections are set to empty collections
   * instead of null, and to prevent some fields from being serialized.
   */
  private static final class SystemArtifactConfigDeserializer
    implements JsonDeserializer<ArtifactConfig>, JsonSerializer<ArtifactConfig> {
    private static final Type PLUGINS_TYPE = new TypeToken<Set<PluginClass>>() { }.getType();
    private static final Type PARENTS_TYPE = new TypeToken<Set<ArtifactRange>>() { }.getType();

    @Override
    public ArtifactConfig deserialize(JsonElement json, Type typeOfT,
                                            JsonDeserializationContext context) throws JsonParseException {
      if (!json.isJsonObject()) {
        throw new JsonParseException("Config file must be a JSON Object.");
      }
      JsonObject obj = json.getAsJsonObject();

      // deserialize fields
      Set<ArtifactRange> parents = context.deserialize(obj.get("parents"), PARENTS_TYPE);
      parents = parents == null ? Collections.<ArtifactRange>emptySet() : parents;
      Set<PluginClass> plugins = context.deserialize(obj.get("plugins"), PLUGINS_TYPE);
      plugins = plugins == null ? Collections.<PluginClass>emptySet() : plugins;

      return new ArtifactConfig(parents, plugins);
    }

    @Override
    public JsonElement serialize(ArtifactConfig src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject obj = new JsonObject();
      obj.add("parents", context.serialize(src.getParents()));
      obj.add("plugins", context.serialize(src.getPlugins()));
      return obj;
    }
  }
}
