/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.namespace;

import com.google.inject.Singleton;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory implementation of {@link NamespaceAdmin}. This class only handles namespaces,
 * does not handle datasets and apps,
 * so when you delete a namespace using this client, only the namespace will be deleted, the apps and datasets will
 * stay, and will have to be deleted separately.
 */
@Singleton
public class InMemoryNamespaceAdmin implements NamespaceAdmin {

  private final Map<NamespaceId, NamespaceMeta> namespaces = new LinkedHashMap<>();

  @Override
  public synchronized List<NamespaceMeta> list() {
    return Collections.unmodifiableList(new ArrayList<>(namespaces.values()));
  }

  @Override
  public synchronized NamespaceMeta get(NamespaceId namespaceId) throws Exception {
    NamespaceMeta meta = namespaces.get(namespaceId);
    if (meta == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    return meta;
  }

  @Override
  public synchronized boolean exists(NamespaceId namespaceId) {
    return namespaces.containsKey(namespaceId);
  }

  @Override
  public synchronized void create(NamespaceMeta metadata) throws Exception {
    NamespaceId id = new NamespaceId(metadata.getName());
    NamespaceMeta existing = namespaces.putIfAbsent(id, metadata);
    if (existing != null) {
      throw new NamespaceAlreadyExistsException(id);
    }
  }

  @Override
  public synchronized void delete(NamespaceId namespaceId) throws Exception {
    if (namespaces.remove(namespaceId) == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }

  @Override
  public void deleteDatasets(NamespaceId namespaceId) {
    // No-op, we're not managing apps and datasets within InMemoryNamespaceAdmin yet
  }

  @Override
  public synchronized void updateProperties(NamespaceId namespaceId, NamespaceMeta namespaceMeta) throws Exception {
    if (namespaces.replace(namespaceId, namespaceMeta) == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
  }
}
