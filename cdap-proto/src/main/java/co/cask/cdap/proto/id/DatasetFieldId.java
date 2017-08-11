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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a field of data that belongs to a dataset.
 */
public class DatasetFieldId extends FieldEntityId implements ParentedId<DatasetId> {
  private final String dataset;
  private transient Integer hashCode;

  public DatasetFieldId(String namespace, String dataset, String field) {
    super(namespace, field);
    if (dataset == null) {
      throw new NullPointerException("Dataset ID cannot be null.");
    }
    ensureValidDatasetId("dataset", dataset);
    this.dataset = dataset;
  }

  public String getDataset() {
    return dataset;
  }

  public DatasetId getDatasetId() {
    return new DatasetId(namespace, dataset);
  }

  @Override
  public String getEntityName() {
    return getField();
  }

  @Override
  public DatasetId getParent() {
    return new DatasetId(namespace, dataset);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    DatasetFieldId that = (DatasetFieldId) o;
    return Objects.equals(this.namespace, that.namespace) &&
      Objects.equals(this.dataset, that.dataset) &&
      Objects.equals(this.field, that.field);
  }

  @Override
  public int hashCode() {
    if (hashCode == null) {
      hashCode = Objects.hash(super.hashCode(), namespace, dataset, field);
    }
    return hashCode;
  }

  @Override
  public Id.DatasetInstance toId() { // no reason to actually implement this...
    return getParent().toId();
  }

  public static DatasetFieldId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new DatasetFieldId(next(iterator, "namespace"), next(iterator, "dataset"),
                       nextAndEnd(iterator, "field"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, dataset, field));
  }
}
