/*
 * Copyright 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.elastic;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The document format that is indexed in Elastic.
 */
public class MetadataDocument {
  private final String type;
  private final List<KeyValue> entity;
  private final Set<Tag> tags;
  private final Set<Property> properties;

  private MetadataDocument(String type, List<KeyValue> entity, Set<Tag> tags, Set<Property> properties) {
    this.type = type;
    this.entity = entity;
    this.tags = tags;
    this.properties = properties;
  }

  /**
   * Create a builder for a MetadataDocument.
   */
  public static Builder of(MetadataEntity entity) {
    return new Builder(entity);
  }

  /**
   * @return the type of the entity
   */
  public String getType() {
    return type;
  }

  /**
   * @return all the parts of the entity as key/values
   */
  public List<KeyValue> getEntity() {
    return entity;
  }

  /**
   * @return all tags
   */
  public Set<Tag> getTags() {
    return tags;
  }

  /**
   * @return the properties
   */
  public Set<Property> getProperties() {
    return properties;
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
    return Objects.equals(type, that.type) &&
      Objects.equals(entity, that.entity) &&
      Objects.equals(tags, that.tags) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, entity, tags, properties);
  }

  @Override
  public String toString() {
    return "MetadataDocument{" +
      "type=" + type +
      "entity=" + entity +
      ", tags=" + tags +
      ", properties=" + properties +
      '}';
  }

  /**
   * Merge with another MetadataDocument. The other document overrides in case of conflict.
   *
   * @return a new documents representing the result of the merge
   */
  public MetadataDocument merge(MetadataDocument doc) {
    Builder builder = new Builder(this);
    doc.tags.forEach(builder::addTag);
    doc.properties.forEach(builder::addProperty);
    return builder.build();
  }

  /**
   * Remove some metadata, as indicated by the given predicates.
   *
   * @return a new documents representing the result of the removal
   */
  public MetadataDocument remove(Predicate<Tag> keepTag, Predicate<Property> keepProperty) {
    return new MetadataDocument(type, entity,
                                tags.stream().filter(keepTag::apply).collect(Collectors.toSet()),
                                properties.stream().filter(keepProperty::apply).collect(Collectors.toSet()));
  }

  /**
   * Represents a tag.
   */
  public static class Tag {
    private final String scope;
    private final String name;

    public Tag(String scope, String name) {
      this.scope = scope;
      this.name = name;
    }

    public String getScope() {
      return scope;
    }

    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Tag tag = (Tag) o;
      return Objects.equals(scope, tag.scope) &&
        Objects.equals(name, tag.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scope, name);
    }

    @Override
    public String toString() {
      return scope + ':' + name;
    }
  }

  /**
   * Represents a property.
   */
  public static final class Property extends Tag {
    private final String value;

    public Property(String scope, String name, String value) {
      super(scope, name);
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Property property = (Property) o;
      return Objects.equals(value, property.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), value);
    }

    @Override
    public String toString() {
      return super.toString() + '=' + value;
    }
  }

  /**
   * Represents a part of the entity.
   */
  public static final class KeyValue {
    private final String key;
    private final String value;

    KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyValue keyValue = (KeyValue) o;
      return Objects.equals(key, keyValue.key) &&
        Objects.equals(value, keyValue.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public String toString() {
      return key + '=' + value;
    }
  }

  /**
   * A builder for MetadataDocuments.
   */
  public static class Builder {
    private final String type;
    private final List<KeyValue> idParts = new ArrayList<>();
    private final Set<Tag> tags = new HashSet<>();
    private final Map<Tag, Property> properties = new HashMap<>();

    private Builder(MetadataEntity entity) {
      type = entity.getType();
      for (MetadataEntity.KeyValue kv : entity) {
        idParts.add(new KeyValue(kv.getKey(), kv.getValue()));
      }
    }

    private Builder(MetadataDocument doc) {
      type = doc.getType();
      idParts.addAll(doc.getEntity());
      tags.addAll(doc.getTags());
      doc.properties.forEach(prop -> properties.put(new Tag(prop.getScope(), prop.getName()), prop));
    }

    public Builder addTag(MetadataScope scope, String name) {
      addTag(scope.name(), name);
      return this;
    }

    private Builder addTag(String scope, String name) {
      tags.add(new Tag(scope, name));
      return this;
    }

    private Builder addTag(Tag tag) {
      tags.add(tag);
      return this;
    }

    public Builder addProperty(MetadataScope scope, String name, String value) {
      properties.put(new Tag(scope.name(), name), new Property(scope.name(), name, value));
      return this;
    }

    private Builder addProperty(Property prop) {
      properties.put(new Tag(prop.getScope(), prop.getName()), prop);
      return this;
    }

    public MetadataDocument build() {
      return new MetadataDocument(type, idParts, tags, new HashSet<>(properties.values()));
    }
  }
}
