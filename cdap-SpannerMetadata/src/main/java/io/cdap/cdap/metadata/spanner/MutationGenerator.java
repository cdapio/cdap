/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.metadata.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.ScopedName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MutationGenerator {

  private static final Gson gson = new Gson();
  private static final String METADATA_PROPS_TABLE = "metadata_props";
  private static final String NESTED_NAME_FIELD = "props_name";
  private static final String NESTED_SCOPE_FIELD = "props_scope";
  private static final String NESTED_VALUE_FIELD = "props_value";

  /**
   * Generates a list of Spanner mutations to write metadata.
   *
   * @param entity           The metadata entity.
   * @param expectVersion    The expected version for optimistic concurrency.
   * @param originalMetadata The metadata object.
   * @return A list of Spanner mutations.
   * @throws IOException If there's an error processing the metadata.
   */
  public List<Mutation> generateMutations(MetadataEntity entity, Long expectVersion, Metadata originalMetadata)
    throws IOException {
    List<Mutation> mutations = new ArrayList<>();
    String parentMetadataId = toDocumentId(entity);
    long newVersion = (expectVersion == null ? 1L : expectVersion + 1);

    // Create a new Metadata object with the added property, if it doesn't exist.
    Metadata updatedMetadata = addPropertyIfNotExists(originalMetadata
    );

    // Use the 'updatedMetadata' object for all subsequent operations
    mutations.add(createParentMutation(entity, updatedMetadata, parentMetadataId, newVersion));

    addPropsAndTagsMutations(mutations, updatedMetadata.getProperties(MetadataScope.USER),
                             updatedMetadata.getTags(MetadataScope.USER), parentMetadataId, MetadataScope.USER);
    addPropsAndTagsMutations(mutations, updatedMetadata.getProperties(MetadataScope.SYSTEM),
                             updatedMetadata.getTags(MetadataScope.SYSTEM), parentMetadataId, MetadataScope.SYSTEM);

    addEntityNameMutation(mutations, entity, parentMetadataId);

    return mutations;
  }

  /**
   * Creates a new Metadata object containing a new property if it doesn't already exist.
   * If the property already exists, it returns the original, unmodified metadata object.
   *
   * @param originalMetadata The original, immutable Metadata object.
   * @return A new Metadata object with the added property, or the original object.
   */
  private Metadata addPropertyIfNotExists(Metadata originalMetadata) {
    if (originalMetadata.getProperties(MetadataScope.USER).containsKey("new_property_name")) {
      return originalMetadata;
    }
    Set<ScopedName> allTags = new HashSet<>(originalMetadata.getTags());
    Map<ScopedName, String> allProperties = new HashMap<>(originalMetadata.getProperties());
    allProperties.put(new ScopedName(MetadataScope.USER, "new_property_name"), "new_property_value");
    return new Metadata(allTags, allProperties);
  }

  /**
   * Creates the parent mutation for the Metadata table.
   */
  private Mutation createParentMutation(MetadataEntity entity, Metadata metadata,
                                        String parentMetadataId, long newVersion) {
    return Mutation.newInsertOrUpdateBuilder("Metadata")
      .set("metadata_id").to(parentMetadataId)
      .set("Namespace").to(entity.getValue(MetadataEntity.NAMESPACE))
      .set("Entity_type").to(entity.getType())
      .set("Name").to(entity.getValue(entity.getType()))
      .set("metadata_column").to(gson.toJson(metadata))
      .set("VERSION").to(newVersion)
      .set("user").to(buildSearchString(metadata.getProperties(MetadataScope.USER),
                                        metadata.getTags(MetadataScope.USER)))
      .set("system").to(buildSearchString(metadata.getProperties(MetadataScope.SYSTEM),
                                          metadata.getTags(MetadataScope.SYSTEM)))
      .build();
  }

  /**
   * Adds mutations for properties and tags for a given scope.
   */
  private void addPropsAndTagsMutations(List<Mutation> mutations, Map<String, String> properties,
                                        Set<String> tags, String parentId, MetadataScope scope) {
    // Mutations for individual properties
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      mutations.add(createPropMutation(parentId, scope, entry.getKey(), entry.getValue()));
      if ("schema".equalsIgnoreCase(entry.getKey())) {
        addSchemaMutations(mutations, parentId, scope, entry.getValue());
      }
    }

    // Mutation for all tags in the scope
    if (!tags.isEmpty()) {
      mutations.add(createPropMutation(parentId, scope, "tags", String.join(" ", tags).toLowerCase()));
    }

    // Mutation for property names summary
    if (!properties.isEmpty()) {
      mutations.add(createPropMutation(parentId, scope, "properties", String.join(" ",
                                                                                  properties.keySet()).toLowerCase()));
    }
  }

  /**
   * Adds mutations for the schema property.
   */
  private void addSchemaMutations(List<Mutation> mutations, String parentId, MetadataScope scope, String schemaJson) {
    try {
      JsonObject schema = gson.fromJson(schemaJson, JsonObject.class);
      if (schema.has("name")) {
        mutations.add(createPropMutation(parentId, scope, "schema", schema.get("name").
          getAsString().toLowerCase()));
      }
      if (schema.has("fields")) {
        JsonArray fields = schema.getAsJsonArray("fields");
        StringBuilder schemaFields = new StringBuilder();
        for (JsonElement fieldElement : fields) {
          JsonObject fieldObject = fieldElement.getAsJsonObject();
          if (fieldObject.has("name") && fieldObject.has("type")) {
            schemaFields.append(fieldObject.get("name").getAsString().toLowerCase())
              .append(":")
              .append(fieldObject.get("type").getAsString().toLowerCase())
              .append(" ");
          }
        }
        if (schemaFields.length() > 0) {
          mutations.add(createPropMutation(parentId, scope, "schema", schemaFields.toString().trim()));
        }
      }
    } catch (Exception e) {
      // Log the error appropriately
      System.err.println("Error parsing schema JSON: " + e.getMessage());
    }
  }

  /**
   * Adds a mutation for the entity name.
   */
  private void addEntityNameMutation(List<Mutation> mutations, MetadataEntity entity, String parentId) {
    String entityName = entity.getValue(entity.getType());
    if (entityName != null) {
      mutations.add(createPropMutation(parentId, MetadataScope.SYSTEM, "entity-name", entityName.toLowerCase()));
    }
  }

  /**
   * Helper method to create a property mutation.
   */
  private Mutation createPropMutation(String parentId, MetadataScope scope, String name, String value) {
    return Mutation.newInsertOrUpdateBuilder(METADATA_PROPS_TABLE)
      .set("metadata_id").to(parentId)
      .set(NESTED_SCOPE_FIELD).to(scope.name())
      .set(NESTED_NAME_FIELD).to(name.toLowerCase())
      .set(NESTED_VALUE_FIELD).to(value)
      .build();
  }

  /**
   * Builds a concatenated string for full-text search.
   */
  private String buildSearchString(Map<String, String> properties, Set<String> tags) {
    StringBuilder searchString = new StringBuilder();
    tags.forEach(tag -> searchString.append(tag.toLowerCase()).append(" "));
    properties.forEach((key, value) -> searchString.append(key.toLowerCase()).append(":").
      append(value.toLowerCase()).append(" "));
    return searchString.toString().trim();
  }

  /**
   * Generates a document ID from a MetadataEntity.
   */
  private String toDocumentId(MetadataEntity entity) {
    // Implement the logic to generate a document ID
    return entity.getType() + "_" + entity.getValue(entity.getType());
  }
}