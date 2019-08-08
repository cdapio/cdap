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

package io.cdap.cdap.metadata.elastic;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaWalker;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.ScopedName;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The document format that is indexed in Elastic.
 *
 * Note that the mapping for creating the ElasticSearch index corresponds directly
 * to the fields of this document class. Any changes made here need to be reflected
 * there, too: index.mapping.json. See {@link ElasticsearchMetadataStorage#createMappings()}
 * for how it is obtained.
 */
public class MetadataDocument {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataDocument.class);

  private final MetadataEntity entity;
  private final Metadata metadata;
  private final String namespace;
  private final String type;
  private final String name;
  private final Long created;
  private final Long ttl;
  private final boolean hidden;
  private final String user;
  private final String system;
  private final Set<Property> props;

  private MetadataDocument(MetadataEntity entity, Metadata metadata,
                           @Nullable String namespace,
                           String type, String name,
                           @Nullable Long created,
                           @Nullable Long ttl,
                           String user, String system,
                           Set<Property> props) {
    this.entity = entity;
    this.metadata = metadata;
    this.namespace = namespace;
    this.type = type;
    this.name = name;
    this.created = created;
    this.ttl = ttl;
    this.hidden = name.startsWith("_");
    this.user = user;
    this.system = system;
    this.props = props;
  }

  Metadata getMetadata() {
    return metadata;
  }

  /**
   * Create a builder for a MetadataDocument.
   */
  static MetadataDocument of(MetadataEntity entity, Metadata metadata) {
    return new Builder(entity).addMetadata(metadata).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataDocument that = (MetadataDocument) o;
    return hidden == that.hidden &&
      Objects.equals(entity, that.entity) &&
      Objects.equals(metadata, that.metadata) &&
      Objects.equals(namespace, that.namespace) &&
      Objects.equals(type, that.type) &&
      Objects.equals(name, that.name) &&
      Objects.equals(created, that.created) &&
      Objects.equals(ttl, that.ttl) &&
      Objects.equals(user, that.user) &&
      Objects.equals(system, that.system) &&
      Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entity, metadata, namespace, type, name, created, ttl, hidden, user, system, props);
  }

  @Override
  public String toString() {
    return "MetadataDocument{" +
      "entity=" + entity +
      ", metadata=" + metadata +
      ", namespace='" + namespace + '\'' +
      ", type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", created=" + created +
      ", ttl=" + ttl +
      ", hidden=" + hidden +
      ", user='" + user + '\'' +
      ", system='" + system + '\'' +
      ", props=" + props +
      '}';
  }

  /**
   * Represents a property.
   */
  public static final class Property {
    private final String scope;
    private final String name;
    private final String value;
    // the value field's numeric representation, if applicable
    private final Double numericValue;

    Property(String scope, String name, String value) {
      this.scope = scope;
      this.name = name;
      this.value = value;
      this.numericValue = getNumericValue(value);
    }

    @Nullable
    private Double getNumericValue(String value) {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Property property = (Property) o;
      return Objects.equals(scope, property.scope) &&
        Objects.equals(name, property.name) &&
        Objects.equals(value, property.value) &&
          Objects.equals(numericValue, property.numericValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), scope, name, value, numericValue);
    }

    @Override
    public String toString() {
      return scope + ':' + name + '=' + value + '|' + numericValue;
    }
  }

  /**
   * A builder for MetadataDocuments.
   */
  public static class Builder {

    private static final ScopedName SCHEMA_KEY = new ScopedName(MetadataScope.SYSTEM, MetadataConstants.SCHEMA_KEY);
    private static final ScopedName TTL_KEY = new ScopedName(MetadataScope.SYSTEM, MetadataConstants.TTL_KEY);
    private static final ScopedName CREATION_TIME_KEY = new ScopedName(MetadataScope.SYSTEM,
                                                                       MetadataConstants.CREATION_TIME_KEY);

    private final MetadataEntity entity;
    private Metadata metadata = Metadata.EMPTY;
    private final String namespace;
    private final String type;
    private final String name;
    private Long created;
    private Long ttl;
    private final List<String> userTags = new ArrayList<>();
    private final List<String> systemTags = new ArrayList<>();
    private final List<String> userPropertyNames = new ArrayList<>();
    private final List<String> systemPropertyNames = new ArrayList<>();
    private final StringBuilder userText = new StringBuilder();
    private final StringBuilder systemText = new StringBuilder();
    private final Set<Property> properties = new HashSet<>();

    private Builder(MetadataEntity entity) {
      this.entity = entity;
      //noinspection ConstantConditions
      this.namespace = entity.containsKey("namespace") ? entity.getValue("namespace").toLowerCase() : null;
      this.type = entity.getType().toLowerCase();
      //noinspection ConstantConditions
      this.name = entity.getValue(entity.getType()).toLowerCase();
      append(MetadataScope.SYSTEM, this.type);
      append(MetadataScope.SYSTEM, this.name);
      addProperty(new ScopedName(MetadataScope.SYSTEM, this.type), this.name);
    }

    private void append(MetadataScope scope, String text) {
      (MetadataScope.USER == scope ? userText : systemText).append(' ').append(text);
    }

    private void addTag(ScopedName tag) {
      String name = tag.getName().toLowerCase();
      append(tag.getScope(), name);
      (MetadataScope.USER == tag.getScope() ? userTags : systemTags).add(name);
    }

    private void addProperty(ScopedName key, String value) {
      String name = key.getName().toLowerCase();
      value = value.toLowerCase();
      if (SCHEMA_KEY.equals(key)) {
        value = parseSchema(entity, value);
      }
      MetadataScope scope = key.getScope();
      append(scope, value);
      properties.add(new Property(scope.name(), name, value));
      (MetadataScope.USER == key.getScope() ? userPropertyNames : systemPropertyNames).add(name);
      checkForBuiltInLong(CREATION_TIME_KEY, key, value).ifPresent(x -> created = x);
      checkForBuiltInLong(TTL_KEY, key, value).ifPresent(x -> ttl = x);
    }

    @VisibleForTesting
    static String parseSchema(MetadataEntity entity, String schemaStr) {
      try {
        Schema schema = Schema.parseJson(schemaStr);
        StringBuilder builder = new StringBuilder();
        SchemaWalker.walk(schema, (field, subSchema) -> {
          if (field != null) {
            String type = (subSchema.isNullable() ? subSchema.getNonNullable() : subSchema).getType().toString();
            builder.append(field).append(' ')
              .append(field).append(MetadataConstants.KEYVALUE_SEPARATOR).append(type).append(' ');
          }
        });
        return builder.toString();
      } catch (Exception e) {
        LOG.warn("Unable to parse schema '{}' for entity {}. Indexing as plain text.", schemaStr, entity);
        return schemaStr;
      }
    }

    Optional<Long> checkForBuiltInLong(ScopedName builtIn, ScopedName key, String value) {
      if (key.equals(builtIn)) {
        try {
          return Optional.of(Long.parseLong(value));
        } catch (NumberFormatException e) {
          LOG.warn("Unable to parse property {} as long. Skipping indexing of {} for entity {}.",
                   builtIn, builtIn.getName(), entity, e);
        }
      }
      return Optional.empty();
    }

    Builder addMetadata(Metadata metadata) {
      this.metadata = metadata;
      metadata.getTags().forEach(this::addTag);
      metadata.getProperties().forEach(this::addProperty);
      return this;
    }

    MetadataDocument build() {
      properties.add(
        new Property(MetadataScope.USER.name(), MetadataConstants.TAGS_KEY,
                     Strings.collectionToDelimitedString(userTags, " ")));
      properties.add(
        new Property(MetadataScope.SYSTEM.name(), MetadataConstants.TAGS_KEY,
                     Strings.collectionToDelimitedString(systemTags, " ")));
      properties.add(
        new Property(MetadataScope.USER.name(), MetadataConstants.PROPERTIES_KEY,
                     Strings.collectionToDelimitedString(userPropertyNames, " ")));
      properties.add(
        new Property(MetadataScope.SYSTEM.name(), MetadataConstants.PROPERTIES_KEY,
                     Strings.collectionToDelimitedString(systemPropertyNames, " ")));
      return
        new MetadataDocument(entity, metadata, namespace, type, name, created, ttl,
                             userText.toString(), systemText.toString(), properties);
    }
  }
}
