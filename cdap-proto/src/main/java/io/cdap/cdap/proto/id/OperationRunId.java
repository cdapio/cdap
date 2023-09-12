/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.proto.id;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.element.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies an operation run.
 */
public class OperationRunId extends NamespacedEntityId implements ParentedId<NamespaceId> {

  private final String run;

  public OperationRunId(String namespace, String run) {
    super(namespace, EntityType.OPERATION_RUN);
    if (run == null) {
      throw new NullPointerException("run id cannot be null.");
    }
    this.run = run;
  }

  @Override
  public String getEntityName() {
    return getRun();
  }

  public String getRun() {
    return this.run;
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, namespace)
        .appendAsType(MetadataEntity.APPLICATION, run)
        .build();
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    OperationRunId that = (OperationRunId) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(run, that.run);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, run);
  }

  @SuppressWarnings("unused")
  public static OperationRunId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new OperationRunId(next(iterator, "namespace"), next(iterator, "run"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, run));
  }

  public static OperationRunId fromString(String string) {
    return EntityId.fromString(string, OperationRunId.class);
  }
}
