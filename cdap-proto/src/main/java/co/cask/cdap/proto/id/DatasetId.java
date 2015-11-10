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
 * Uniquely identifies a dataset.
 */
public class DatasetId extends EntityId implements NamespacedId, ParentedId<NamespaceId> {
  private final String namespace;
  private final String dataset;

  public DatasetId(String namespace, String dataset) {
    super(EntityType.DATASET);
    this.namespace = namespace;
    this.dataset = dataset;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getDataset() {
    return dataset;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public Id toId() {
    return Id.DatasetInstance.from(namespace, dataset);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    DatasetId datasetId = (DatasetId) o;
    return Objects.equals(namespace, datasetId.namespace) &&
      Objects.equals(dataset, datasetId.dataset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, dataset);
  }

  @SuppressWarnings("unused")
  public static DatasetId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new DatasetId(next(iterator, "namespace"), nextAndEnd(iterator, "dataset"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, dataset);
  }

  public static DatasetId fromString(String string) {
    return EntityId.fromString(string, DatasetId.class);
  }
}
