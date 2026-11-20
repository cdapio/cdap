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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaWalker;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.ScopedName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class to process and prepare metadata fields for optimized storage.
 * This class encapsulates the logic for flattening text fields, parsing schemas,
 * and extracting specific properties for relational storage.
 */
public class FormattedMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(FormattedMetadata.class);

  // Fields directly mapped to metadata table columns
  private final String namespace;
  private final String type;
  private final String name;
  private final Long created;
  private final String userText;
  private final String systemText;
  private final Set<Property> metadataProps;

  /**
   * Creates an instance of FormattedMetadata from a MetadataEntity and Metadata.
   * This is the public entry point for creating formatted metadata.
   *
   * @param entity   the metadata entity being processed.
   * @param metadata the metadata to be associated with the entity.
   * @return a new instance of FormattedMetadata.
   * @throws IOException if schema parsing fails.
   */
  public static FormattedMetadata from(MetadataEntity entity, Metadata metadata) throws IOException {
    return new FormattedMetadata(entity, metadata);
  }

  private FormattedMetadata(MetadataEntity entity, Metadata metadata) throws IOException {
    this.namespace = Optional.ofNullable(entity.getValue("namespace"))
      .orElse("default");
    this.type = entity.getType().toLowerCase();
    this.name = Objects.requireNonNull(entity.getValue(entity.getType())).toLowerCase();

    Map<ScopedName, String> properties = metadata.getProperties();
    String schemaJson = properties.get(new ScopedName(MetadataScope.SYSTEM, MetadataConstants.SCHEMA_KEY));
    Map<String, Set<Property>> schemaProperty = reformatSchemaProperty(schemaJson);
    Map<String, Set<Property>> reformatedProperties = reformatProperties(properties);
    Set<Property> reformatedPropertiesWithValue = reformatedProperties
      .getOrDefault("extractedProperties", Collections.emptySet());
    reformatedPropertiesWithValue.addAll(schemaProperty.getOrDefault("schemaAndFieldNames",
                                                                     Collections.emptySet()));
    Set<ScopedName> tags = metadata.getTags();
    Set<Property> reformatedTags = reformatTags(tags);

    this.metadataProps = new HashSet<>();
    this.metadataProps.addAll(reformatedPropertiesWithValue);
    this.metadataProps.addAll(reformatedTags);
    this.metadataProps.add(new Property(MetadataScope.SYSTEM.name(), this.type, this.name));
    this.systemText = buildText(reformatedPropertiesWithValue, tags, MetadataScope.SYSTEM) + " " + type;
    this.metadataProps.addAll(reformatedProperties.getOrDefault("propertyNames", Collections.emptySet()));
    this.metadataProps.addAll(schemaProperty.getOrDefault("fieldProperties", Collections.emptySet()));
    this.userText = buildText(reformatedPropertiesWithValue, tags, MetadataScope.USER);
    this.created = parseCreationTime(reformatedPropertiesWithValue).orElse(null);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public Optional<Long> getCreated() {
    return Optional.ofNullable(created);
  }

  public String getUserText() {
    return userText;
  }

  public String getSystemText() {
    return systemText;
  }

  public Set<Property> getMetadataProps() {
    return metadataProps;
  }

  /**
   * Processes properties and extract property names into a Set.
   *
   * @param properties The original properties from the metadata object.
   * @return A Set containing Property objects.
   */
  private Map<String, Set<Property>> reformatProperties(Map<ScopedName, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<Property> extracted = new HashSet<>(properties.size());
    Set<String> propertyNames = new HashSet<>();
    for (Map.Entry<ScopedName, String> entry : properties.entrySet()) {
      ScopedName key = entry.getKey();
      String name = key.getName().toLowerCase();
      String scope = key.getScope().name();
      String value = entry.getValue().toLowerCase();
      extracted.add(new Property(scope, name, value));
      propertyNames.add(name);
    }

    String allPropertiesValue = String.join(" ", propertyNames);
    Set<Property> propertyValue = new HashSet<>();
    propertyValue.add(new Property(MetadataScope.SYSTEM.name(),
                                          MetadataConstants.PROPERTIES_KEY, allPropertiesValue));

    Map<String, Set<Property>> result = new HashMap<>();
    result.put("extractedProperties", extracted);
    result.put("propertyNames", propertyValue);

    return result;

  }

  /**
   * Extracts tags into the set format.
   */
  private Set<Property> reformatTags(Set<ScopedName> tags) {
    return tags.stream()
      .map(tag -> new Property(tag.getScope().name(), MetadataConstants.TAGS_KEY, tag.getName()))
      .collect(Collectors.toSet());
  }

  /**
   * Builds a single, space-delimited string of text from properties and tags
   * for a given scope.
   */
  private String buildText(Set<Property> properties, Set<ScopedName> tags, MetadataScope scope) {
    String scopeProperties = properties.stream()
      .filter(property -> Objects.equals(property.getScope(), scope.toString()))
      .map(property -> property.getValue().toLowerCase())
      .collect(Collectors.joining(" "));

    String scopeTags = tags.stream()
      .filter(tag -> tag.getScope() == scope)
      .map(tag -> tag.getName().toLowerCase())
      .collect(Collectors.joining(" "));

    return Stream.of(scopeProperties, scopeTags)
      .filter(s -> !s.isEmpty())
      .collect(Collectors.joining(" "));
  }

  /**
   * Finds and parses the creation time from the processed properties.
   */
  private Optional<Long> parseCreationTime(Set<Property> properties) {
    if (properties == null || properties.isEmpty()) {
      return Optional.empty();
    }

    String expectedScope = MetadataScope.SYSTEM.name();
    String expectedName = MetadataConstants.CREATION_TIME_KEY;
    for (Property property : properties) {
      if (expectedScope.equalsIgnoreCase(property.getScope()) && expectedName.equalsIgnoreCase(property.getName())) {
        try {
          return Optional.of(Long.parseLong(property.getValue()));
        } catch (NumberFormatException e) {
          LOG.warn("Unable to parse property value '{}' as a long for the creation time key. Skipping.",
                   property.getValue(), e);
          return Optional.empty();
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Parses a schema JSON string into a concise, human-readable format.
   *
   * @param schemaStr The raw JSON string representing the schema.
   * @return A formatted string (e.g., "schemaname:TYPE field1:TYPE1 field2:TYPE2").
   * @throws IOException if the schema string cannot be parsed.
   */
  private Map<String, Set<Property>> reformatSchemaProperty(String schemaStr) throws IOException {
    if (schemaStr == null) {
      return Collections.emptyMap();
    }

    Set<Property> fieldProperties = new HashSet<>();
    Set<Property> schemaAndFieldNames = new HashSet<>();
    Schema schema = Schema.parseJson(schemaStr);

    List<String> formattedFields = new ArrayList<>();
    SchemaWalker.walk(schema, (fieldName, fieldSchema) -> {
      if (fieldName != null) {
        Schema nonNullableSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
        String typeName = nonNullableSchema.getType().toString().toLowerCase();
        fieldProperties.add(new Property(
          MetadataScope.SYSTEM.name(),
          fieldName.toLowerCase(),
          typeName));
        formattedFields.add(fieldName.toLowerCase() + ":" + typeName.toLowerCase());
      }
    });

    String schemaProperties = formattedFields.isEmpty() ? "" : String.join(" ", formattedFields);
    schemaAndFieldNames.add(new Property(
      MetadataScope.SYSTEM.name(),
      "schema",
      schemaProperties));

    Map<String, Set<Property>> result = new HashMap<>();
    result.put("fieldProperties", fieldProperties);
    result.put("schemaAndFieldNames", schemaAndFieldNames);

    return result;
  }

  /**
   * Represents a property for the Spanner metadata_props table.
   */
  static class Property {
    private final String scope;
    private final String name;
    private final String value;

    Property(String scope, String name, String value) {
      this.scope = scope;
      this.name = name;
      this.value = value;
    }

    public String getScope() {
      return scope;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    /**
     * Checks if this Property is equal to another object.
     */
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Property property = (Property) o;
      return Objects.equals(scope, property.scope)
        && Objects.equals(name, property.name)
        && Objects.equals(value, property.value);
    }

    public int hashCode() {
      return Objects.hash(scope, name, value);
    }

    public String toString() {
      return scope + ':' + name + ':' + value;
    }
  }
}

