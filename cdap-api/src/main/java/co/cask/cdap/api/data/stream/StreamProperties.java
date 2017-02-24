/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.api.data.stream;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the properties of a stream.
 */
public class StreamProperties {

  private final Long ttl;
  private final FormatSpecification format;
  @SerializedName("notification.threshold.mb")
  private final Integer notificationThresholdMB;
  private final String description;
  @SerializedName("principal")
  private final String ownerPrincipal;
  private final Map<String, String> properties;

  protected StreamProperties(@Nullable Long ttl,
                             @Nullable FormatSpecification format,
                             @Nullable Integer notificationThresholdMB,
                             @Nullable String description,
                             @Nullable String ownerPrincipal,
                             @Nullable Map<String, String> properties) {
    this.ttl = ttl;
    this.format = format;
    this.ownerPrincipal = ownerPrincipal;
    this.notificationThresholdMB = notificationThresholdMB;
    this.description = description;
    this.properties = properties == null || properties.isEmpty()
      ? Collections.<String, String>emptyMap() : Collections.unmodifiableMap(new HashMap<>(properties));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(StreamProperties props) {
    return new Builder(props);
  }

  /**
   * @return The time to live in seconds for events in this stream.
   */
  @Nullable
  public Long getTTL() {
    return ttl;
  }

  /**
   * @return The format specification for the stream.
   */
  @Nullable
  public FormatSpecification getFormat() {
    return format;
  }

  /**
   *
   * @return The notification threshold of the stream
   */
  @Nullable
  public Integer getNotificationThresholdMB() {
    return notificationThresholdMB;
  }

  /**
   * @return The description of the stream
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * @return The stream owner principal
   */
  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }

  /**
   * @return The additional properties as a map
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @return a specific property
   */
  @Nullable
  public String getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamProperties)) {
      return false;
    }

    StreamProperties that = (StreamProperties) o;

    return Objects.equals(ttl, that.ttl) &&
      Objects.equals(format, that.format) &&
      Objects.equals(notificationThresholdMB, that.notificationThresholdMB) &&
      Objects.equals(description, that.description) &&
      Objects.equals(ownerPrincipal, that.ownerPrincipal) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ttl, format, notificationThresholdMB, description, ownerPrincipal, properties);
  }

  @Override
  public String toString() {
    return "StreamProperties{" +
      "ttl=" + ttl +
      ", format=" + format +
      ", notificationThresholdMB=" + notificationThresholdMB +
      ", description='" + description + '\'' +
      ", ownerPrincipal='" + ownerPrincipal + '\'' +
      ", properties=" + properties +
      '}';
  }

  /**
   * A builder for stream properties.
   */
  public static class Builder {
    Long ttl;
    String format;
    Schema schema;
    Map<String, String> formatSettings = new HashMap<>();
    Integer notificatonThreshold;
    String description;
    String principal;
    String groupName;
    String permissions;
    Map<String, String> properties = new HashMap<>();

    private Builder() { }

    private Builder(StreamProperties props) {
      ttl = props.getTTL();
      if (props.getFormat() != null) {
        format = props.getFormat().getName();
        schema = props.getFormat().getSchema();
        formatSettings.putAll(props.getFormat().getSettings());
      }
      notificatonThreshold = props.getNotificationThresholdMB();
      description = props.getDescription();
      principal = props.getOwnerPrincipal();
      properties.putAll(props.properties);
    }

    public Builder setTTL(long ttl) {
      this.ttl = ttl;
      return this;
    }

    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public Builder addFormatSetting(String key, String value) {
      this.formatSettings.put(key, value);
      return this;
    }

    public Builder setFormatSpec(FormatSpecification spec) {
      if (spec.getName() == null) {
        throw new IllegalArgumentException("Format specification must have a format name.");
      }
      this.format = spec.getName();
      this.schema = spec.getSchema();
      this.formatSettings = new HashMap<>(spec.getSettings());
      return this;
    }

    public Builder setPrincipal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder setGroupName(String groupName) {
      this.groupName = groupName;
      return this;
    }

    public Builder setPermissions(String permissions) {
      this.permissions = permissions;
      return this;
    }

    public Builder setNotificatonThreshold(Integer notificatonThreshold) {
      this.notificatonThreshold = notificatonThreshold;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    public Builder addProperty(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public StreamProperties build() {
      if (format == null && !formatSettings.isEmpty()) {
        throw new IllegalArgumentException("A format name is required if format formatSettings are given.");
      }
      if (format == null && schema != null) {
        throw new IllegalArgumentException("A format name is required if a schema is given.");
      }
      return new StreamProperties(
        ttl, format == null ? null : new FormatSpecification(format, schema, new HashMap<>(formatSettings)),
        notificatonThreshold, description, principal, properties);
    }
  }
}
