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

package co.cask.cdap.store;

import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link NamespaceStore} used in test cases.
 */
public class InMemoryNamespaceStore implements NamespaceStore {

  private final Map<NamespaceId, NamespaceMeta> namespaces = new HashMap<>();

  @Nullable
  @Override
  public NamespaceMeta create(NamespaceMeta metadata) {
    return namespaces.put(metadata.getNamespaceId(), metadata);
  }

  @Override
  public void update(NamespaceMeta metadata) {
    if (namespaces.containsKey(metadata.getNamespaceId())) {
      namespaces.put(metadata.getNamespaceId(), metadata);
    }
  }

  @Nullable
  @Override
  public NamespaceMeta get(NamespaceId id) {
    return namespaces.get(id);
  }

  @Nullable
  @Override
  public NamespaceMeta delete(NamespaceId id) {
    return namespaces.remove(id);
  }

  @Override
  public List<NamespaceMeta> list() {
    return new ArrayList<>(namespaces.values());
  }
}
