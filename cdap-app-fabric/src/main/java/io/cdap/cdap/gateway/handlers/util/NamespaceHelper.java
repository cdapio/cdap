/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.util;

import com.google.common.base.Throwables;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Helper class for Namespace operations.
 */
public class NamespaceHelper {

  private NamespaceHelper() {
  }

  /**
   * Validates that the namespace exists and gets the NamespaceId
   *
   * @param namespaceQueryAdmin query admin for namespace operations
   * @param namespace the namespace to validate
   * @return NamespaceId
   *
   * @throws NamespaceNotFoundException if namespace is not found
   */
  public static NamespaceId validateNamespace(NamespaceQueryAdmin namespaceQueryAdmin, String namespace)
      throws NamespaceNotFoundException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    try {
      namespaceQueryAdmin.get(namespaceId);
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP to interact with namespaces.
      // Within AppFabric, NamespaceAdmin is bound to DefaultNamespaceAdmin which directly interacts with MDS.
      // Hence, this should never happen.
      throw Throwables.propagate(e);
    }
    return namespaceId;
  }
}
