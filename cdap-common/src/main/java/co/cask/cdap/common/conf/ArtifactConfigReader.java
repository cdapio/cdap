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
import co.cask.cdap.api.artifact.InvalidArtifactRangeException;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRanges;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Reads files into {@link ArtifactConfig ArtifactConfigs} for a specific namespace.
 */
public class ArtifactConfigReader {
  private final LoadingCache<Id.Namespace, Gson> gsonCache;

  public ArtifactConfigReader() {
    this.gsonCache = CacheBuilder.newBuilder().build(
      new CacheLoader<Id.Namespace, Gson>() {
        @Override
        public Gson load(Id.Namespace namespace) throws Exception {
          return new GsonBuilder()
            .registerTypeAdapter(ArtifactRange.class, new ArtifactRangeDeserializer(namespace))
            .registerTypeAdapter(ArtifactConfig.class, new ArtifactConfigDeserializer())
            .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
            .create();
        }
      });
  }

  /**
   * Read the contents of the given file and deserialize it into an ArtifactConfig.
   *
   * @param namespace the namespace of the artifact this config file is for
   * @param configFile the config file to read
   * @return the contents of the file parsed as an ArtifactConfig
   * @throws IOException if there was a problem reading the file, for example if the file does not exist.
   * @throws InvalidArtifactException if there was a problem deserializing the file contents due to some invalid
   *                                  format or unexpected value.
   */
  public ArtifactConfig read(Id.Namespace namespace, File configFile) throws IOException, InvalidArtifactException {
    String fileName = configFile.getName();
    try (Reader reader = Files.newReader(configFile, Charsets.UTF_8)) {
      try {
        ArtifactConfig config = gsonCache.getUnchecked(namespace).fromJson(reader, ArtifactConfig.class);

        // check namespaces in parents are either system or the specified namespace
        for (ArtifactRange parent : config.getParents()) {
          NamespaceId parentNamespace = new NamespaceId(parent.getNamespace());
          if (!NamespaceId.SYSTEM.equals(parentNamespace) && !namespace.toEntityId().equals(parentNamespace)) {
            throw new InvalidArtifactException(String.format("Invalid parent %s. Parents must be in the same " +
              "namespace or a system artifact.", parent));
          }
        }
        return config;
      } catch (JsonSyntaxException e) {
        throw new InvalidArtifactException(String.format("%s contains invalid json: %s", fileName, e.getMessage()), e);
      } catch (JsonParseException e) {
        throw new InvalidArtifactException(String.format("Unable to parse %s: %s", fileName, e.getMessage()), e);
      }
    }
  }

  /**
   * Deserializer for ArtifactRange in a ArtifactConfig. Artifact ranges are expected to be able to be
   * parsed via {@link ArtifactRanges#parseArtifactRange(String)}} or
   * {@link ArtifactRanges#parseArtifactRange(String, String)}.
   */
  private static class ArtifactRangeDeserializer implements JsonDeserializer<ArtifactRange> {
    private final Id.Namespace namespace;

    ArtifactRangeDeserializer(Id.Namespace namespace) {
      this.namespace = namespace;
    }

    @Override
    public ArtifactRange deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {
      if (!json.isJsonPrimitive()) {
        throw new JsonParseException("ArtifactRange should be a string.");
      }

      String rangeStr = json.getAsString();
      try {
        if (rangeStr.indexOf(':') > 0) {
          return ArtifactRanges.parseArtifactRange(rangeStr);
        } else {
          return ArtifactRanges.parseArtifactRange(namespace.toEntityId().getNamespace(), rangeStr);
        }
      } catch (InvalidArtifactRangeException e) {
        throw new JsonParseException(e.getMessage());
      }
    }
  }

  /**
   * Deserializer for ArtifactConfig. Used to make sure collections are set to empty collections
   * instead of null.
   */
  private static final class ArtifactConfigDeserializer implements JsonDeserializer<ArtifactConfig> {
    private static final Type PLUGINS_TYPE = new TypeToken<Set<PluginClass>>() { }.getType();
    private static final Type PARENTS_TYPE = new TypeToken<Set<ArtifactRange>>() { }.getType();
    private static final Type PROPERTIES_TYPE = new TypeToken<Map<String, String>>() { }.getType();

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
      Map<String, String> properties = context.deserialize(obj.get("properties"), PROPERTIES_TYPE);
      properties = properties == null ? Collections.<String, String>emptyMap() : properties;

      return new ArtifactConfig(parents, plugins, properties);
    }
  }
}
