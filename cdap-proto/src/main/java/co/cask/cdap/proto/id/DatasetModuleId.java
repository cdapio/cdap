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
 * Uniquely identifies a dataset module.
 */
public class DatasetModuleId extends EntityId implements NamespacedId, ParentedId<NamespaceId> {
  private final String namespace;
  private final String module;

  public DatasetModuleId(String namespace, String module) {
    super(EntityType.DATASET_MODULE);
    this.namespace = namespace;
    this.module = module;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getModule() {
    return module;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public Id toId() {
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
    return Objects.hash(super.hashCode(), namespace, module);
  }

  @SuppressWarnings("unused")
  public static DatasetModuleId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new DatasetModuleId(next(iterator, "namespace"), nextAndEnd(iterator, "module"));
  }

  @Override
  protected Iterable<String> toIdParts() {
    return ImmutableList.of(namespace, module);
  }

  public static DatasetModuleId fromString(String string) {
    return EntityId.fromString(string, DatasetModuleId.class);
  }
}
