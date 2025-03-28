package io.cdap.cdap.metadata.spanner;


import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.Map;
import java.util.HashMap;
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
    private final boolean hidden;
    private final String user;
    private final String system;
    private final Set<Property> props;

    private SpannerMetadataDocument(MetadataEntity entity, Metadata metadata,
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

    public static SpannerMetadataDocument of(MetadataEntity entity, Metadata metadata) {
        return new Builder(entity).addMetadata(metadata).build();
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SpannerMetadataDocument that = (SpannerMetadataDocument) o;
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

    public String getNamespace() {
        return namespace;
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

    public Set<Property> getProps() {
        return props;
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

    public static final class Property {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Property property = (Property) o;
            return Objects.equals(scope, property.scope) &&
                    Objects.equals(name, property.name) &&
                    Objects.equals(value, property.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, name, value);
        }

        @Override
        public String toString() {
            return scope + ':' + name + '=' + value;
        }
    }


    public static class Builder {

        private static final ScopedName SCHEMA_KEY = new ScopedName(MetadataScope.SYSTEM, MetadataConstants.SCHEMA_KEY);
        private static final ScopedName TTL_KEY = new ScopedName(MetadataScope.SYSTEM, MetadataConstants.TTL_KEY);
        private static final ScopedName CREATION_TIME_KEY = new ScopedName(MetadataScope.SYSTEM, MetadataConstants.
                CREATION_TIME_KEY);

        private final MetadataEntity entity;
        private final String namespace;
        private final String type;
        private final String name;
        private final List<String> userTags = new ArrayList<>();
        private final List<String> systemTags = new ArrayList<>();
        private final List<String> userPropertyNames = new ArrayList<>();
        private final List<String> systemPropertyNames = new ArrayList<>();
        private final StringBuilder userText = new StringBuilder();
        private final StringBuilder systemText = new StringBuilder();
        private final Set<Property> properties = new HashSet<>();
        private Metadata metadata = Metadata.EMPTY;
        private Long created;
        private Long ttl;
        private static final Gson GSON = new GsonBuilder().create();


        public Builder(MetadataEntity entity) {
            this.entity = entity;
            this.namespace = entity.containsKey("namespace") ? entity.getValue("namespace").toLowerCase() : null;
            this.type = entity.getType().toLowerCase();
            this.name = entity.getValue(entity.getType()).toLowerCase();
            append(MetadataScope.SYSTEM, this.type);
            append(MetadataScope.SYSTEM, this.name);
            addProperty(new ScopedName(MetadataScope.SYSTEM, this.type), this.name);
        }

        public static Builder builder(MetadataEntity entity) { // ADDED THIS METHOD
            return new Builder(entity);
        }

        @VisibleForTesting
        static String parseSchema(MetadataEntity entity, String schemaStr) {
            try {
                Schema schema = Schema.parseJson(schemaStr);
                List<Map<String, Object>> fields = new ArrayList<>();
                SchemaWalker.walk(schema, (field, subSchema) -> {
                    if (field != null) {
                        Map<String, Object> fieldMap = new HashMap<>();
                        fieldMap.put("name", field); // Use field directly as it's the field name
                        fieldMap.put("type", (subSchema.isNullable() ? subSchema.getNonNullable() : subSchema).
                                getType().toString().toLowerCase());
                        fields.add(fieldMap);
                    }
                });

                // Filter out records from fields
                List<Map<String, Object>> filteredFields = new ArrayList<>();
                for (Map<String, Object> field : fields) {
                    if (!"record".equalsIgnoreCase((String) field.get("type"))) {
                        filteredFields.add(field);
                    }
                }

                Map<String, Object> schemaJson = new HashMap<>();
                schemaJson.put("type", "record");
                schemaJson.put("name", "etlSchemaBody");
                schemaJson.put("fields", filteredFields); // Use the filtered fields
                return GSON.toJson(schemaJson);
            } catch (Exception e) {
                LOG.warn("Unable to parse schema '{}' for entity {}. Indexing as plain text.", schemaStr, entity);
                return schemaStr;
            }
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

        Optional<Long> checkForBuiltInLong(ScopedName builtIn, ScopedName key, String value) {
            if (key.equals(builtIn)) {
                try {
                    return Optional.of(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to parse property {} as long. Skipping indexing of {} for entity {}.", builtIn,
                            builtIn.getName(), entity, e);
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
                    userText.toString(), systemText.toString(), propertiesSet);
        }
    }
}

