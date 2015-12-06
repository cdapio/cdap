/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a namespace.
 */
public class NamespaceId extends EntityId {
  private final String namespace;

  public static final NamespaceId DEFAULT = new NamespaceId("default");
  public static final NamespaceId SYSTEM = new NamespaceId("system");

  public NamespaceId(String namespace) {
    super(EntityType.NAMESPACE);
    this.namespace = namespace;
  }

  public String getNamespace() {
    return namespace;
  }

  public NamespacedArtifactId artifact(String artifact, String version) {
    return new NamespacedArtifactId(namespace, artifact, version);
  }

  public ApplicationId app(String application) {
    return new ApplicationId(namespace, application);
  }

  public DatasetId dataset(String dataset) {
    return new DatasetId(namespace, dataset);
  }

  public DatasetModuleId datasetModule(String module) {
    return new DatasetModuleId(namespace, module);
  }

  public DatasetTypeId datasetType(String type) {
    return new DatasetTypeId(namespace, type);
  }

  public StreamId stream(String stream) {
    return new StreamId(namespace, stream);
  }

  @Override
  public Id toId() {
    return Id.Namespace.from(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    NamespaceId that = (NamespaceId) o;
    return Objects.equals(namespace, that.namespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace);
  }

  @SuppressWarnings("unused")
  public static NamespaceId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new NamespaceId(nextAndEnd(iterator, "namespace"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace);
  }

  public static NamespaceId fromString(String string) {
    return EntityId.fromString(string, NamespaceId.class);
  }
}
