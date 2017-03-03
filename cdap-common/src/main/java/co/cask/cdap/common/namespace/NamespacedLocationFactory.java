/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;

import java.io.IOException;

/**
 * Interface for managing locations in a namespace
 */
public interface NamespacedLocationFactory {

  /**
   * Returns the base {@link Location} for the specified namespace on the filesystem
   *
   * @param namespaceId the namespace for which base location is desired
   * @return {@link Location} for the specified namespace on the filesystem
   */
  Location get(NamespaceId namespaceId) throws IOException;

  /**
   * Returns the base {@link Location} for the specified namespace on the filesystem
   *
   * @param namespaceMeta the {@link NamespaceMeta metadata} of the namespace for which base location is desired
   * @return {@link Location} for the specified namespace on the filesystem
   */
  Location get(NamespaceMeta namespaceMeta) throws IOException;
}
