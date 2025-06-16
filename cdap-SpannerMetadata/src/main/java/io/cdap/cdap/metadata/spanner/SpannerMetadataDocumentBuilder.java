package io.cdap.cdap.metadata.spanner;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaWalker;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.ScopedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A helper class to process and prepare metadata fields for optimized storage
 * in Google Cloud Spanner, similar to how Elasticsearch's MetadataDocument prepares data.
 * This class encapsulates the logic for flattening text fields, parsing schemas,
 * and extracting specific properties for relational storage.
 */
public class SpannerMetadataDocumentBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerMetadataDocumentBuilder.class);
  private static final Gson GSON = new Gson(); // Use GSON if needed for internal parsing

  private final MetadataEntity entity;
  private final Metadata metadata;

  // Fields directly mapped to main metadata table columns
  private final String namespace;
  private final String type;
  private final String name;
  private Long created;
  private Long ttl;
  private final StringBuilder userText = new StringBuilder();
  private final StringBuilder systemText = new StringBuilder();

  // Fields for the separate metadata_props table
  private final Set<Property> propertiesForSpannerPropsTable = new HashSet<>();

  public SpannerMetadataDocumentBuilder(MetadataEntity entity, Metadata metadata) {
    this.entity = entity;
    this.metadata = metadata;

    // Initialize core entity fields
    this.namespace = entity.containsKey("namespace") ? entity.getValue("namespace").toLowerCase() : null;
    this.type = entity.getType().toLowerCase();
    this.name = entity.getValue(entity.getType()).toLowerCase();

    // Add entity type and name to system text for general search
    systemText.append(' ').append(this.type);
    systemText.append(' ').append(this.name);
    // Optionally, also add as a property for structured querying on entity type/name
    propertiesForSpannerPropsTable.add(new Property(MetadataScope.SYSTEM.name(), this.type, this.name));

    // Process all metadata properties and tags
    processMetadata();
  }

  /**
   * Processes all properties and tags from the Metadata object
   * to populate the builder's internal fields (text, properties for child table).
   */
  private void processMetadata() {
    // Process properties
    for (Map.Entry<ScopedName, String> entry : metadata.getProperties().entrySet()) {
      ScopedName key = entry.getKey();
      String value = entry.getValue();
      String propName = key.getName().toLowerCase();
      String propValueForText = value.toLowerCase(); // Value for concatenated text fields
      String propValueForPropsTable = value;  // Value for individual property rows (can be original case or formatted)

      // Special handling for 'schema' property: format for main props value, and extract for search
      if (MetadataConstants.SCHEMA_KEY.equals(key)) {
        // For the main 'schema' property value in metadata_props, use the concise format
        propValueForPropsTable = formatSchemaConcise(value);
        // For the full-text search string, we might still want a more verbose, searchable format
        propValueForText = parseSchemaForSearch(value);

        // Additionally, extract 'schema_name' and 'schema_fields' as separate properties for detailed querying
        extractSchemaDerivedProperties(value);
      }

      // Append to user/system text builders for full-text search
      (MetadataScope.USER == key.getScope() ? userText : systemText).append(' ').append(propValueForText);

      // Add to properties set for spanner_props table
      propertiesForSpannerPropsTable.add(new Property(key.getScope().name(), propName, propValueForPropsTable));

      // Check for built-in long properties like creation time and TTL
      checkForBuiltInLong(ScopedName.fromString(MetadataConstants.CREATION_TIME_KEY), key,
                          value).ifPresent(x -> created = x);
      checkForBuiltInLong(ScopedName.fromString(MetadataConstants.TTL_KEY), key, value).ifPresent(x -> ttl = x);
    }

    // Process tags
    for (ScopedName tag : metadata.getTags()) {
      String tagName = tag.getName().toLowerCase();
      // Append tags to appropriate text builder for full-text search
      (MetadataScope.USER == tag.getScope() ? userText : systemText).append(' ').append(tagName);
      // Add tags as individual properties to the props table (useful for structured tag queries)
      propertiesForSpannerPropsTable.add(new Property(tag.getScope().name(), MetadataConstants.TAGS_KEY, tagName));
    }
  }

  /**
   * Parses the schema JSON and formats it into a concise, human-readable string
   * for the main 'schema' property value in metadata_props.
   * Example: `etlSchemaBody:RECORD body:STRING`
   */
  private String formatSchemaConcise(String schemaStr) {
    try {
      JsonObject schemaJson = GSON.fromJson(schemaStr, JsonObject.class);
      JsonElement nameElement = schemaJson.get("name");
      JsonElement typeElement = schemaJson.get("type");
      JsonArray fields = schemaJson.getAsJsonArray("fields");

      StringBuilder formattedSchemaBuilder = new StringBuilder();

      if (nameElement != null && nameElement.isJsonPrimitive()) {
        formattedSchemaBuilder.append(nameElement.getAsString().toLowerCase());
        if (typeElement != null && typeElement.isJsonPrimitive()) {
          formattedSchemaBuilder.append(":").append(typeElement.getAsString().toUpperCase());
        }
      }

      if (fields != null && fields.size() > 0) {
        if (formattedSchemaBuilder.length() > 0) {
          formattedSchemaBuilder.append(" ");
        }
        boolean firstField = true;
        for (JsonElement fieldElement : fields) {
          if (fieldElement.isJsonObject()) {
            JsonObject fieldObject = fieldElement.getAsJsonObject();
            JsonElement fieldNameElement = fieldObject.get("name");
            JsonElement fieldTypeElement = fieldObject.get("type");

            if (fieldNameElement != null && fieldNameElement.isJsonPrimitive() &&
              fieldTypeElement != null && fieldTypeElement.isJsonPrimitive()) {
              if (!firstField) {
                formattedSchemaBuilder.append(" ");
              }
              formattedSchemaBuilder.append(fieldNameElement.getAsString().toLowerCase())
                .append(":")
                .append(fieldTypeElement.getAsString().toUpperCase());
              firstField = false;
            }
          }
        }
      }
      return formattedSchemaBuilder.toString().trim();
    } catch (Exception e) {
      LOG.warn("Error formatting schema '{}' into concise string. Falling back to original. Error: {}",
               schemaStr, e.getMessage());
      return schemaStr; // Fallback to original JSON string on error
    }
  }

  /**
   * Parses the schema JSON and flattens it into a searchable text string.
   * Example: "etlSchemaBody record body string"
   */
  private String parseSchemaForSearch(String schemaStr) {
    try {
      Schema schema = Schema.parseJson(schemaStr);
      StringBuilder builder = new StringBuilder();
      // Add schema name and top-level type for search
      if (schema.getRecordName() != null) {
        builder.append(schema.getRecordName().toLowerCase()).append(" ");
      }
      builder.append(schema.getType().toString().toLowerCase()).append(" ");

      SchemaWalker.walk(schema, (field, subSchema) -> {
        if (field != null) {
          String type = (subSchema.isNullable() ? subSchema.getNonNullable() : subSchema).getType().toString();
          // Append both field name and fieldName:fieldType for broader search
          builder.append(field).append(" ").append(field).append(MetadataConstants.KEYVALUE_SEPARATOR).
            append(type).append(" ");
        }
      });
      return builder.toString().trim().toLowerCase();
    } catch (Exception e) {
      LOG.warn("Unable to parse schema '{}' for entity {}. Indexing as plain text for search. Error: {}",
               schemaStr, entity, e.getMessage());
      return schemaStr.toLowerCase(); // Fallback to plain text, lowercased
    }
  }

  /**
   * Extracts derived schema properties like 'schema_name' and 'schema_fields'
   * and adds them to the set of properties for the metadata_props table.
   */
  private void extractSchemaDerivedProperties(String schemaStr) {
    try {
      JsonObject schemaJson = GSON.fromJson(schemaStr, JsonObject.class);
      JsonElement nameElement = schemaJson.get("name");
      if (nameElement != null && nameElement.isJsonPrimitive()) {
        propertiesForSpannerPropsTable.add(new Property(MetadataScope.SYSTEM.name(), "schema_name",
                                                        nameElement.getAsString().toLowerCase()));
      }

      JsonArray fields = schemaJson.getAsJsonArray("fields");
      if (fields != null) {
        StringBuilder schemaFieldsValue = new StringBuilder();
        for (JsonElement fieldElement : fields) {
          if (fieldElement.isJsonObject()) {
            JsonObject fieldObject = fieldElement.getAsJsonObject();
            JsonElement fieldNameElement = fieldObject.get("name");
            JsonElement fieldTypeElement = fieldObject.get("type");
            if (fieldNameElement != null && fieldNameElement.isJsonPrimitive() &&
              fieldTypeElement != null && fieldTypeElement.isJsonPrimitive()) {
              schemaFieldsValue.append(fieldNameElement.getAsString().toLowerCase()).append(":")
                .append(fieldTypeElement.getAsString().toLowerCase()).append(" ");
            }
          }
        }
        if (schemaFieldsValue.length() > 0) {
          propertiesForSpannerPropsTable.add(new Property(MetadataScope.SYSTEM.name(), "schema_fields",
                                                          schemaFieldsValue.toString().trim()));
        }
      }
    } catch (Exception e) {
      LOG.warn("Error extracting derived schema properties from '{}'. Error: {}", schemaStr, e.getMessage());
      // No need to throw, just log and continue
    }
  }

  /**
   * Checks if a property is a built-in long value (like creation time or TTL)
   * and attempts to parse it.
   */
  private Optional<Long> checkForBuiltInLong(ScopedName builtIn, ScopedName key, String value) {
    if (key.equals(builtIn)) {
      try {
        return Optional.of(Long.parseLong(value));
      } catch (NumberFormatException e) {
        LOG.warn("Unable to parse property {} as long for entity {}. Skipping.", builtIn, entity, e);
      }
    }
    return Optional.empty();
  }

  // --- Getters for processed data ---
  public String getNamespace() { return namespace; }
  public String getType() { return type; }
  public String getName() { return name; }
  public Optional<Long> getCreated() { return Optional.ofNullable(created); }
  public Optional<Long> getTtl() { return Optional.ofNullable(ttl); }
  public String getUserText() { return userText.toString().trim(); }
  public String getSystemText() { return systemText.toString().trim(); }
  public Set<Property> getPropertiesForSpannerPropsTable() { return propertiesForSpannerPropsTable; }


  /**
   * Represents a property for the Spanner metadata_props table.
   * This is public static so it can be accessed from SpannerMetadataStorage.
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

    public String getScope() { return scope; }
    public String getName() { return name; }
    public String getValue() { return value; }

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
}