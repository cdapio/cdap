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

package co.cask.cdap.etl.api.lineage.field;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An EndPoint in an operation represents the source or sink of the data,
 * along with the namespace in which they exists. When namespace is not provided,
 * the namespace for an EndPoint is considered to be same as the namespace in which
 * pipeline runs.
 */
public class EndPoint {
  private final String namespace;
  private final String name;

  private EndPoint(String name) {
    this.name = name;
    namespace = null;
  }

  private EndPoint(String namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  /**
   * @return the namespace name if it is explicitly provided while creating this EndPoint,
   * otherwise {@code null} is returned
   */
  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return the name of the {@link EndPoint}
   */
  public String getName() {
    return name;
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
   * @param namespace the name of the namespace.
   * @param name the name of the EndPoint
   * @return the EndPoint
   */
  public static EndPoint of(String namespace, String name) {
    return new EndPoint(namespace, name);
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
      && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name);
  }
}
