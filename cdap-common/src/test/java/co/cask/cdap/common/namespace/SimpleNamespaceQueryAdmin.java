/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.namespace;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link NamespaceQueryAdmin} which simply returns a {@link NamespaceMeta} with
 * a {@link NamespaceConfig} that contains a custom mapping if it is created with one in its constructor else
 * a {@link NamespaceMeta} with given NamespaceId.
 */
public class SimpleNamespaceQueryAdmin implements NamespaceQueryAdmin {
  private final Map<String, NamespaceMeta> customNSMap;

  public SimpleNamespaceQueryAdmin() {
    this.customNSMap = ImmutableMap.of();
  }

  public SimpleNamespaceQueryAdmin(Map<String, NamespaceMeta> customNSMap) {
    this.customNSMap = ImmutableMap.copyOf(customNSMap);
  }

  @Override
  public List<NamespaceMeta> list() throws Exception {
    throw new UnsupportedOperationException("Listing of namespaces is not supported.");
  }

  @Override
  public NamespaceMeta get(Id.Namespace namespaceId) throws Exception {
    return customNSMap.containsKey(namespaceId.getId()) ? customNSMap.get(namespaceId.getId()) :
      new NamespaceMeta.Builder().setName(namespaceId.getId()).build();
  }

  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    // We always return true here since this query admin is only for tests classes where we want to work with
    // namespaces without actually creating the namespace. This is why the get of this method always return a meta so
    // we always return true here.
    return true;
  }
}
