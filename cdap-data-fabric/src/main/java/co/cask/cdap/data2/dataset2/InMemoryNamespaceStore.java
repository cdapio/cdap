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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.store.NamespaceStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link NamespaceStore} used in test cases.
 */
public class InMemoryNamespaceStore implements NamespaceStore {

  private final Map<Id.Namespace, NamespaceMeta> namespaces = new HashMap<>();

  @Nullable
  @Override
  public NamespaceMeta create(NamespaceMeta metadata) {
    return namespaces.put(Id.Namespace.from(metadata.getName()), metadata);
  }

  @Override
  public void update(NamespaceMeta metadata) {
    Id.Namespace namespaceId = Id.Namespace.from(metadata.getName());
    if (namespaces.containsKey(namespaceId)) {
      namespaces.put(namespaceId, metadata);
    }
  }

  @Nullable
  @Override
  public NamespaceMeta get(Id.Namespace id) {
    return namespaces.get(id);
  }

  @Nullable
  @Override
  public NamespaceMeta delete(Id.Namespace id) {
    return namespaces.remove(id);
  }

  @Override
  public List<NamespaceMeta> list() {
    return new ArrayList<>(namespaces.values());
  }
}
