/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Request for spec generation
 */
public class ConnectorSpecRequest {
  private static final String CONNECTION_NAME_PREFIX = "${conn(";
  private static final String CONNECTION_NAME_FORMAT = "${conn(%s)}";

  private final String path;
  private final Map<String, String> properties;
  // this is the connection name with macro wrapped, like {conn:(connection-name)}
  private final String connectionWithMacro;

  private ConnectorSpecRequest(@Nullable String path, Map<String, String> properties, String connectionWithMacro) {
    this.path = path;
    this.properties = properties;
    this.connectionWithMacro = connectionWithMacro;
  }

  /**
   * Get the entity path for request, if the path is null, that means the properties contains
   * all the path related configs
   */
  @Nullable
  public String getPath() {
    return path;
  }

  public String getConnectionWithMacro() {
    return connectionWithMacro;
  }

  public Map<String, String> getProperties() {
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

    ConnectorSpecRequest that = (ConnectorSpecRequest) o;
    return Objects.equals(path, that.path) &&
             Objects.equals(properties, that.properties) &&
             Objects.equals(connectionWithMacro, that.connectionWithMacro);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, properties, connectionWithMacro);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ConnectorSpecRequest}
   */
  public static class Builder {
    private String path;
    private Map<String, String> properties;
    private String connectionWithMacro;

    public Builder() {
      this.properties = new HashMap<>();
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setConnection(String connection) {
      this.connectionWithMacro = connection.startsWith(CONNECTION_NAME_PREFIX) ? connection :
                                   String.format(CONNECTION_NAME_FORMAT, connection);
      return this;
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }

    public Builder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public Builder addProperties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public ConnectorSpecRequest build() {
      return new ConnectorSpecRequest(path, properties, connectionWithMacro);
    }
  }
}
