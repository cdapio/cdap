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

package io.cdap.cdap.metadata.spanner;


import com.google.cloud.spanner.Value;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaWalker;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.ScopedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.Objects;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;


/**
 * Represents a metadata document, encapsulating metadata operations and data structure.
 */
public class SpannerMetadataDocument {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataDocument.class);

    private final MetadataEntity entity;
    private final Metadata metadata;
    private final String namespace;
    private final String type;
    private final String name;
    private final Long created;
    private final Long ttl;
    private  final boolean hidden;
    private  final String user;
    private final String system;
    private final String pipelineId;
    private final Set<Property> props;
    private final Long version;

    private SpannerMetadataDocument(MetadataEntity entity, Metadata metadata,
                                    @Nullable String namespace,
                                    String type, String name,
                                    @Nullable Long created,
                                    @Nullable Long ttl,
                                    String user, String system,
                                    String pipelineId,Set<Property> props, Long version) {
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
        this.pipelineId = pipelineId;
        this.props = props;
        this.version = version;
    }

    Metadata getMetadata() {
        return metadata;
    }

    /**
     * Create a builder for a MetadataDocument.
     */
    static SpannerMetadataDocument of(MetadataEntity entity, Metadata metadata) {
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
        SpannerMetadataDocument that = (SpannerMetadataDocument) o;
        return hidden == that.hidden
                && Objects.equals(entity, that.entity)
                && Objects.equals(metadata, that.metadata)
                && Objects.equals(namespace, that.namespace)
                && Objects.equals(type, that.type)
                && Objects.equals(name, that.name)
                && Objects.equals(created, that.created)
                && Objects.equals(ttl, that.ttl)
                && Objects.equals(user, that.user)
                && Objects.equals(system, that.system)
                && Objects.equals(pipelineId, that.pipelineId)
                && Objects.equals(props, that.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity, metadata, namespace, type, name, created, ttl, hidden, user, system,
                props);
    }

    @Override
    public String toString() {
        return "MetadataDocument{"
                + "entity=" + entity
                + ", metadata=" + metadata
                + ", namespace='" + namespace + '\''
                + ", type='" + type + '\''
                + ", name='" + name + '\''
                + ", created=" + created
                + ", ttl=" + ttl
                + ", hidden=" + hidden
                + ", user='" + user + '\''
                + ", system='" + system + '\''
                + ", props=" + props
                + '}';
    }

    public String getNamespace() {
        return namespace;
    }

    public Object getProps() {
        return props;
    }

    public Object getEntity() {
        return entity;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Long getCreated() {
        return created;
    }


    public Long getTtl() {
        return ttl;
    }

    public boolean isHidden() {
        return hidden;
    }

    public String getUser() {
        return user;
    }

    public String getSystem() {
        return system;
    }

    public Long getVersion() {
        return version;
    }


    /**
     * Represents a property.
     */
    public static final class Property {

        private final String scope;
        private final String name;
        private final String value;

        Property(String scope, String name, String value) {
            this.scope = scope;
            this.name = name;
            this.value = value;
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
            return Objects.equals(scope, property.scope)
                    && Objects.equals(name, property.name)
                    && Objects.equals(value, property.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), scope, name, value);
        }

        @Override
        public String toString() {
            return scope + ':' + name + '=' + value;
        }
    }

    /**
     * A builder for MetadataDocuments.
     */
    public static class Builder {

        private static final ScopedName SCHEMA_KEY = new ScopedName(MetadataScope.SYSTEM,
                MetadataConstants.SCHEMA_KEY);
        private static final ScopedName TTL_KEY = new ScopedName(MetadataScope.SYSTEM,
                MetadataConstants.TTL_KEY);
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
        private String pipelineId;
        private Long version;

        private Builder(MetadataEntity entity) {
            this.entity = entity;
            //noinspection ConstantConditions
            this.namespace =
                    entity.containsKey("namespace") ? entity.getValue("namespace").toLowerCase() : null;
            this.type = entity.getType().toLowerCase();
            //noinspection ConstantConditions
            this.name = entity.getValue(entity.getType()).toLowerCase();
            this.pipelineId = namespace + "/" + type + "/" + name;
            this.version = 1L;
            append(MetadataScope.SYSTEM, this.type);
            append(MetadataScope.SYSTEM, this.name);
            addProperty(new ScopedName(MetadataScope.SYSTEM, this.type), this.name);
        }

        private void append(MetadataScope scope, String text) {
            (MetadataScope.USER == scope ? userText : systemText).append(' ').append(text);
        }
        Builder setVersion(Long version) {
            this.version = version;
            return this;
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

        public MetadataEntity getEntity() {
            return entity;
        }

        public Metadata getMetadata() {
            return metadata;
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



        @VisibleForTesting
        static String parseSchema(MetadataEntity entity, String schemaStr) {
            try {
                Schema schema = Schema.parseJson(schemaStr);
                StringBuilder builder = new StringBuilder();
                SchemaWalker.walk(schema, (field, subSchema) -> {
                    if (field != null) {
                        String type = (subSchema.isNullable() ? subSchema.getNonNullable()
                                : subSchema).getType().toString();
                        builder.append(field).append(' ')
                                .append(field).append(MetadataConstants.KEYVALUE_SEPARATOR).append(type)
                                .append(' ');
                    }
                });
                return builder.toString();
            } catch (Exception e) {
                LOG.warn("Unable to parse schema '{}' for entity {}. Indexing as plain text.", schemaStr,
                        entity);
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

        SpannerMetadataDocument build() {
            List<SpannerMetadataDocument.Property> propertiesList = new ArrayList<>(this.properties);

            // Add user tags
            if (!userTags.isEmpty()) {
                propertiesList.add(new SpannerMetadataDocument.Property(
                        MetadataScope.USER.name(),
                        MetadataConstants.TAGS_KEY,
                        String.join(" ", userTags)));
            }

            // Add system tags
            if (!systemTags.isEmpty()) {
                propertiesList.add(new SpannerMetadataDocument.Property(
                        MetadataScope.SYSTEM.name(),
                        MetadataConstants.TAGS_KEY,
                        String.join(" ", systemTags)));
            }

            // Add user properties
            if (!userPropertyNames.isEmpty()) {
                propertiesList.add(new SpannerMetadataDocument.Property(
                        MetadataScope.USER.name(),
                        MetadataConstants.PROPERTIES_KEY,
                        String.join(" ", userPropertyNames)));
            }

            // Add system properties
            if (!systemPropertyNames.isEmpty()) {
                propertiesList.add(new SpannerMetadataDocument.Property(
                        MetadataScope.SYSTEM.name(),
                        MetadataConstants.PROPERTIES_KEY,
                        String.join(" ", systemPropertyNames)));
            }

            // Convert List to Set
            Set<SpannerMetadataDocument.Property> propertiesSet = new HashSet<>(propertiesList);

            return new SpannerMetadataDocument(entity, metadata, namespace, type, name, created, ttl,
                    userText.toString(), systemText.toString(),pipelineId, propertiesSet, version);
        }
    }
}