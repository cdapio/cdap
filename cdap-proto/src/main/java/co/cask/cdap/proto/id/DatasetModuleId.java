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
 * Uniquely identifies a dataset module.
 */
public class DatasetModuleId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  private final String module;
  private transient Integer hashCode;

  public DatasetModuleId(String namespace, String module) {
    super(namespace, EntityType.DATASET_MODULE);
    if (module == null) {
      throw new NullPointerException("Module ID cannot be null.");
    }
    ensureValidDatasetId("dataset module", module);
    this.module = module;
  }

  public String getModule() {
    return module;
  }

  @Override
  public String getEntityName() {
    return getModule();
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public Id.DatasetModule toId() {
    return Id.DatasetModule.from(namespace, module);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    DatasetModuleId that = (DatasetModuleId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(module, that.module);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, module);
    }
    return hashCode;
  }

  @SuppressWarnings("unused")
  public static DatasetModuleId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new DatasetModuleId(next(iterator, "namespace"), nextAndEnd(iterator, "module"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, module));
  }

  public static DatasetModuleId fromString(String string) {
    return EntityId.fromString(string, DatasetModuleId.class);
  }
}
