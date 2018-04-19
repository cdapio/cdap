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

package co.cask.cdap.runtime.spi.provisioner;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Information about a cluster.
 */
public class Cluster {
  private final String name;
  private final ClusterStatus status;
  private final Collection<Node> nodes;
  private final Map<String, String> properties;

  public Cluster(String name, ClusterStatus status, Collection<Node> nodes, Map<String, String> properties) {
    this.name = name;
    this.status = status;
    this.nodes = Collections.unmodifiableCollection(nodes);
    this.properties = properties;
  }

  public Cluster(Cluster existing, ClusterStatus newStatus) {
    this(existing.getName(), newStatus, existing.getNodes(), existing.getProperties());
  }

  public String getName() {
    return name;
  }

  public ClusterStatus getStatus() {
    return status;
  }

  public Collection<Node> getNodes() {
    return nodes;
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

    Cluster that = (Cluster) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(status, that.status) &&
      Objects.equals(nodes, that.nodes) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, status, nodes, properties);
  }

  @Override
  public String toString() {
    return "Cluster{" +
      "name='" + name + '\'' +
      ", status=" + status +
      ", nodes=" + nodes +
      ", properties=" + properties +
      '}';
  }
}
