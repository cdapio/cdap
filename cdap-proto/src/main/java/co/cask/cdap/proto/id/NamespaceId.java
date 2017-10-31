/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a namespace.
 */
public class NamespaceId extends NamespacedEntityId {

  public static final NamespaceId DEFAULT = new NamespaceId("default");
  public static final NamespaceId SYSTEM = new NamespaceId("system");
  public static final NamespaceId CDAP = new NamespaceId("cdap");

  private transient Integer hashCode;

  public NamespaceId(String namespace) {
    super(namespace, EntityType.NAMESPACE);
  }

  @Override
  public String getEntityName() {
    return getNamespace();
  }

  public ArtifactId artifact(String artifact, String version) {
    return new ArtifactId(namespace, artifact, version);
  }

  public ApplicationId app(String application) {
    return new ApplicationId(namespace, application);
  }

  public ApplicationId app(String application, String version) {
    return new ApplicationId(namespace, application, version);
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

  public SecureKeyId secureKey(String keyName) {
    return new SecureKeyId(namespace, keyName);
  }

  public TopicId topic(String topic) {
    return new TopicId(namespace, topic);
  }

  @Override
  public Id.Namespace toId() {
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
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace);
    }
    return hashCode;
  }

  @SuppressWarnings("unused")
  public static NamespaceId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new NamespaceId(nextAndEnd(iterator, "namespace"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.singletonList(namespace);
  }

  public static NamespaceId fromString(String string) {
    return EntityId.fromString(string, NamespaceId.class);
  }
}
