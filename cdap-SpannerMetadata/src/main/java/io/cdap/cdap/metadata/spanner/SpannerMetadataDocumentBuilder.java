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
    this.namespace = entity.containsKey("namespace") ? entity.getValue("namespace") : null;
    this.type = entity.getType();
    this.name = entity.getValue(entity.getType());

    // Add entity type and name to system text for general search
    systemText.append(' ').append(this.type);
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
    // --- START OF CHANGE 1: Prepare to collect property names ---
    // A Set is used to automatically handle any duplicate property names.
    Set<String> propertyNames = new HashSet<>();

    // Process properties
    for (Map.Entry<ScopedName, String> entry : metadata.getProperties().entrySet()) {
      ScopedName key = entry.getKey();
      String value = entry.getValue();
      String propName = key.getName().toLowerCase();
      String propValueForText = value.toLowerCase();
      String propValueForPropsTable = value;

      // --- START OF CHANGE 2: Collect the property name ---
      propertyNames.add(propName);

      // Special handling for 'schema' property
      if (MetadataConstants.SCHEMA_KEY.equals(key.getName())) {
        propValueForPropsTable = formatSchemaConcise(value);
        propValueForText = propValueForPropsTable;
        // The call to extractSchemaDerivedProperties() is removed as per your previous request.
      }

      (MetadataScope.USER == key.getScope() ? userText : systemText).append(' ').append(propValueForText);
      propertiesForSpannerPropsTable.add(new Property(key.getScope().name(), propName, propValueForPropsTable));

      checkForBuiltInLong(ScopedName.fromString(MetadataScope.SYSTEM.name() + MetadataConstants.KEYVALUE_SEPARATOR +
                                                  MetadataConstants.CREATION_TIME_KEY),
                          key, value).ifPresent(x -> created = x);
      checkForBuiltInLong(ScopedName.fromString(MetadataScope.SYSTEM.name() + MetadataConstants.KEYVALUE_SEPARATOR +
                                                  MetadataConstants.TTL_KEY),
                          key, value).ifPresent(x -> ttl = x);
    }

    // Process tags
    for (ScopedName tag : metadata.getTags()) {
      String tagName = tag.getName().toLowerCase();
      (MetadataScope.USER == tag.getScope() ? userText : systemText).append(' ').append(tagName);
      propertiesForSpannerPropsTable.add(new Property(tag.getScope().name(), MetadataConstants.TAGS_KEY, tagName));
    }

    // --- START OF CHANGE 3: Create and add the summary 'properties' property ---
    // After processing all properties, if any were found, create the summary entry.
    if (!propertyNames.isEmpty()) {
      String allPropertiesValue = String.join(" ", propertyNames);
      propertiesForSpannerPropsTable.add(new Property(
        MetadataScope.SYSTEM.name(), // This is system-generated metadata
        MetadataConstants.PROPERTIES_KEY, // Assuming this constant is 'properties'
        allPropertiesValue
      ));
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

            if (fieldNameElement != null && fieldNameElement.isJsonPrimitive() && fieldTypeElement != null) {
              String fieldTypeName = "";
              // --- START OF FIX ---
              // Handle both primitive types (e.g., "string") and union types (e.g., ["string", "null"])
              if (fieldTypeElement.isJsonPrimitive()) {
                fieldTypeName = fieldTypeElement.getAsString();
              } else if (fieldTypeElement.isJsonArray()) {
                // For a union type, find the first non-null type
                for (JsonElement typeInArray : fieldTypeElement.getAsJsonArray()) {
                  if (typeInArray.isJsonPrimitive() && !"null".equalsIgnoreCase(typeInArray.getAsString())) {
                    fieldTypeName = typeInArray.getAsString();
                    break; // Found the type, exit the inner loop
                  }
                }
              }
              // --- END OF FIX ---

              // Only append if we successfully determined a type name
              if (!fieldTypeName.isEmpty()) {
                if (!firstField) {
                  formattedSchemaBuilder.append(" ");
                }
                formattedSchemaBuilder.append(fieldNameElement.getAsString().toLowerCase())
                  .append(":")
                  .append(fieldTypeName.toUpperCase());
                firstField = false;
              }
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
      return scope + ':' + name + ':' + value;
    }
  }
}