/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.common.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Utility to extract a metadata entity from a path in an HTTP request.
 */
public final class MetadataPath {

  // Metadata endpoints uses names like 'apps' to represent application 'namespaces' to represent namespace so this map
  // is needed to convert one into another so that we can create a MetadataEntity appropriately.
  private static final Map<String, String> PATH_TO_ENTITY_COMPONENT = ImmutableMap.<String, String>builder()
    .put("namespaces", MetadataEntity.NAMESPACE)
    .put("apps", MetadataEntity.APPLICATION)
    .put("versions", MetadataEntity.VERSION)
    .put("runs", MetadataEntity.PROGRAM_RUN)
    .put("artifacts", MetadataEntity.ARTIFACT)
    .put("datasets", MetadataEntity.DATASET)
    .build();

  private static final Map<String, String> PATH_TO_PROGRAM_TYPE_MAP =
    Stream.of(
      ProgramType.MAPREDUCE,
      ProgramType.SERVICE,
      ProgramType.SPARK,
      ProgramType.WORKER,
      ProgramType.WORKFLOW
    ).collect(Collectors.toMap(ProgramType::getCategoryName, ProgramType::getPrettyName));

  private static final Set<String> NEEDS_VERSION = ImmutableSet.of(MetadataEntity.APPLICATION);

  private static final Map<String, Set<String>> ENTITY_ID_KEY_FOLLOWERS = ImmutableMap.<String, Set<String>>builder()
    .put("", ImmutableSet.of(MetadataEntity.NAMESPACE))
    .put(MetadataEntity.NAMESPACE, ImmutableSet.of(MetadataEntity.ARTIFACT, MetadataEntity.APPLICATION,
                                                   MetadataEntity.DATASET))
    .put(MetadataEntity.ARTIFACT, ImmutableSet.of(MetadataEntity.VERSION))
    .put(MetadataEntity.APPLICATION, ImmutableSet.of(MetadataEntity.VERSION))
    .put(MetadataEntity.VERSION, ImmutableSet.of(MetadataEntity.SCHEDULE, MetadataEntity.PROGRAM))
    .build();

  private MetadataPath() { }

  public static MetadataEntity getMetadataEntityFromPath(String uri, String prefix, String suffix,
                                                         @Nullable String entityType) {
    int startIndex = uri.indexOf(prefix) + prefix.length();
    if (startIndex < prefix.length()) {
      throw new IllegalArgumentException(String.format("Path '%s' does not contain expected prefix '%s'", uri, prefix));
    }
    int endIndex = uri.lastIndexOf(suffix);
    if (endIndex < 0) {
      throw new IllegalArgumentException(String.format("Path '%s' does not contain expected suffix '%s'", uri, suffix));
    }
    String path = uri.substring(uri.indexOf(prefix) + prefix.length(), uri.lastIndexOf(suffix));
    String[] parts = path.split("/");
    if (parts.length % 2 != 0) {
      throw new IllegalArgumentException(String.format("Path '%s' does not have an even number of components and " +
                                                         "cannot be parsed as a metadata entity", path));
    }

    MetadataEntity.Builder builder = MetadataEntity.builder();
    String lastKey = "";

    // map keys to built-in entity types if they follow the entity id hierarchy
    for (int i = 0; i < parts.length; i += 2) {
      // if ever the current key does not match this set, we have a custom entity
      Set<String> allowedInHierarchy = ENTITY_ID_KEY_FOLLOWERS.get(lastKey);
      String key = parts[i];
      String value = parts[i + 1];
      // is this a component of a known type at a position compatible with the entity id hierarchy?
      // if so, map it to its equivalent key. For example, .../apps/<name>... maps to APPLICATION=<name>.
      String mapped = PATH_TO_ENTITY_COMPONENT.get(key);
      if (mapped != null && allowedInHierarchy != null && allowedInHierarchy.contains(mapped)) {
        // the version is never the type of an entity
        if (MetadataEntity.VERSION.equals(mapped)) {
          builder.append(mapped, value);
        } else {
          append(builder, entityType, mapped, value);
        }
        lastKey = mapped;
        // if this is a type that needs version and is not followed by the version > add default version
        if (NEEDS_VERSION.contains(lastKey) && (i + 2 >= parts.length || !"versions".equals(parts[i + 2]))) {
          // version can never be the type, so directly append this to the metadata entity
          builder.append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION);
          lastKey = MetadataEntity.VERSION;
        }
        continue;
      }
      // for programs, the URL has apps/<app>/[versions]/<version>/]<type>/<name> but in the
      // metadata entity it translates to TYPE=type,PROGRAM=name. But only if we are in the known entity hierarchy
      if (allowedInHierarchy != null && allowedInHierarchy.contains(MetadataEntity.PROGRAM)) {
        String programType = PATH_TO_PROGRAM_TYPE_MAP.get(key);
        if (programType != null) {
          // TYPE is never the entity type, PROGRAM always is
          builder.append(MetadataEntity.TYPE, programType);
          builder.appendAsType(MetadataEntity.PROGRAM, value);
          lastKey = MetadataEntity.PROGRAM;
          continue;
        }
      }
      // nothing to adjust, simply append this to the entity
      append(builder, entityType, key, value);
      lastKey = key;
    }
    MetadataEntity entity = builder.build();
    if (entityType != null && !entity.getType().equals(entityType)) {
      throw new IllegalArgumentException(String.format(
        "Path '%s' yields entity %s of type '%s', but type '%s' was requested",
        path, entity, entity.getType(), entityType));
    }
    return entity;
  }

  // append to an entity, while paying attention to the requested entity type
  private static void append(MetadataEntity.Builder builder, @Nullable String entityType, String key, String value) {
    if (entityType == null || entityType.isEmpty()) {
      // if a type is not provided then keep appending as type to update the type on every append
      builder.appendAsType(key, value);
    } else {
      if (entityType.equalsIgnoreCase(key)) {
        // if a type was provided and this key is the type then appendAsType
        builder.appendAsType(key, value);
      } else {
        builder.append(key, value);
      }
    }
  }
}
