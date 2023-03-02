/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.api.lineage.field;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An EndPoint in an operation represents the source or destination of the data, along with the
 * namespace in which they exists. When namespace is not provided, the namespace for an EndPoint is
 * considered to be same as the namespace in which program runs.
 */
public class EndPoint {

  @Nullable
  private final String namespace;
  @Nullable
  private final String name;
  private final Map<String, String> properties;

  private EndPoint(String name) {
    this(null, name, Collections.emptyMap());
  }

  private EndPoint(String namespace, String name) {
    this(namespace, name, Collections.emptyMap());
  }

  private EndPoint(String namespace, String name, Map<String, String> properties) {
    this.namespace = namespace;
    this.name = name;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  /**
   * @return the namespace name if it is explicitly provided while creating this EndPoint, otherwise
   *     {@code null} is returned. Also, in the case where in a pipeline a field is dropped, the
   *     dropped EndPointField is mapped to a blank EndPointField with namespace set to null.
   */
  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return the name of the {@link EndPoint} Name can be null in the case where in a pipeline a
   *     field is dropped, and the dropped EndPointField is mapped to a blank EndPointField with
   *     name set to null.
   */
  @Nullable
  public String getName() {
    return name;
  }

  /**
   * @return the properties of the {@link EndPoint}. e.g. location
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Return the EndPoint as defined by the provided name.
   *
   * @param name the name of the EndPoint
   * @return the EndPoint
   */
  public static EndPoint of(String name) {
    return new EndPoint(name);
  }

  /**
   * Return the EndPoint as defined by the provided namespace and name.
   *
   * @param namespace the name of the namespace
   * @param name the name of the EndPoint
   * @return the EndPoint
   */
  public static EndPoint of(String namespace, String name) {
    return new EndPoint(namespace, name);
  }

  /**
   * Return the EndPoint as defined by the provided namespace, name and properties.
   *
   * @param namespace the name of the namespace
   * @param name the name of the EndPoint
   * @param properties the properties of the EndPoint. e.g. location
   * @return the EndPoint
   */
  public static EndPoint of(String namespace, String name, Map<String, String> properties) {
    return new EndPoint(namespace, name, properties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EndPoint that = (EndPoint) o;

    return Objects.equals(namespace, that.namespace)
        && Objects.equals(name, that.name)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, properties);
  }

  @Override
  public String toString() {
    return "EndPoint{"
        + "namespace='" + namespace + '\''
        + ", name='" + name + '\''
        + ", properties='" + properties + '\''
        + '}';
  }
}
