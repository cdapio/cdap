package co.cask.cdap.api.metadata;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by rsinha on 1/22/18.
 */
public class MetadataRecord {
  private final MetadataEntity entityId;
  private final MetadataScope scope;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link MetadataRecord} in the specified {@link MetadataScope}.
   */
  public MetadataRecord(MetadataEntity entityId, MetadataScope scope) {
    this(entityId, scope, Collections.<String, String>emptyMap(), Collections.<String>emptySet());
  }

  /**
   * Returns a new {@link MetadataRecord} from the specified existing {@link MetadataRecord}.
   */
  public MetadataRecord(MetadataRecord other) {
    this(other.getEntityId(), other.getScope(), other.getProperties(), other.getTags());
  }

  public MetadataRecord(MetadataEntity entityId, MetadataScope scope, Map<String, String> properties,
                        Set<String> tags) {
    this.entityId = entityId;
    this.scope = scope;
    this.properties = properties;
    this.tags = tags;
  }

  public MetadataEntity getEntityId() {
    return entityId;
  }

  public MetadataScope getScope() {
    return scope;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Set<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MetadataRecord that = (MetadataRecord) o;

    if (entityId != null ? !entityId.equals(that.entityId) : that.entityId != null) return false;
    if (scope != that.scope) return false;
    if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
    return tags != null ? tags.equals(that.tags) : that.tags == null;
  }

  @Override
  public int hashCode() {
    int result = entityId != null ? entityId.hashCode() : 0;
    result = 31 * result + (scope != null ? scope.hashCode() : 0);
    result = 31 * result + (properties != null ? properties.hashCode() : 0);
    result = 31 * result + (tags != null ? tags.hashCode() : 0);
    return result;
  }
}
