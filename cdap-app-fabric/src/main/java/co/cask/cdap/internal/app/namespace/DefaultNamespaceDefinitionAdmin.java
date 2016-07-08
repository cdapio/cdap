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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.NamespaceDefinitionAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.store.NamespaceStore;
import com.google.inject.Inject;

import java.util.List;

/**
 * Default implementation of {@link NamespaceDefinitionAdmin} to query namespace details.
 */
public class DefaultNamespaceDefinitionAdmin implements NamespaceDefinitionAdmin {

  protected final NamespaceStore nsStore;

  @Inject
  public DefaultNamespaceDefinitionAdmin(NamespaceStore nsStore) {
    this.nsStore = nsStore;
  }

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  @Override
  public List<NamespaceMeta> list() throws Exception {
    return nsStore.list();
  }

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   */
  @Override
  public NamespaceMeta get(Id.Namespace namespaceId) throws Exception {
    NamespaceMeta ns = nsStore.get(namespaceId);
    if (ns == null) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    return ns;
  }

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    try {
      get(namespaceId);
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }
}
