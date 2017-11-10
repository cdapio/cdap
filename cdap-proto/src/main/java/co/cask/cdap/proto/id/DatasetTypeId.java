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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a dataset type.
 */
public class DatasetTypeId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  private final String type;
  private transient Integer hashCode;

  public DatasetTypeId(String namespace, String type) {
    super(namespace, EntityType.DATASET_TYPE);
    if (type == null) {
      throw new NullPointerException("Type cannot be null.");
    }
    ensureValidDatasetId("dataset type", type);
    this.type = type;
  }

  public String getType() {
    return type;
  }

  @Override
  public String getEntityName() {
    return getType();
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
    DatasetTypeId that = (DatasetTypeId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, type);
    }
    return hashCode;
  }

  @SuppressWarnings("unused")
  public static DatasetTypeId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new DatasetTypeId(next(iterator, "namespace"), nextAndEnd(iterator, "type"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, type));
  }

  @Override
  public Id.DatasetType toId() {
    return Id.DatasetType.from(namespace, type);
  }

  public static DatasetTypeId fromString(String string) {
    return EntityId.fromString(string, DatasetTypeId.class);
  }
}
