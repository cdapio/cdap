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

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.List;

/**
 * Admin for querying namespace. For namespace manipulation operation see {@link NamespaceAdmin}
 */
public interface NamespaceQueryAdmin {
  /**
   * Lists all namespaces.
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  List<NamespaceMeta> list() throws Exception;

  /**
   * Gets details of a namespace.
   *
   * @param namespaceId the {@link NamespaceId} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   */
  NamespaceMeta get(NamespaceId namespaceId) throws Exception;

  /**
   * Checks if the specified namespace exists.
   *
   * @param namespaceId the {@link NamespaceId} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  boolean exists(NamespaceId namespaceId) throws Exception;
}
