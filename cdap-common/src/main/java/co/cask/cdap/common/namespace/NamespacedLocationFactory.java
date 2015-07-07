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

package co.cask.cdap.common.namespace;


import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface for managing locations in a namespace
 */
public interface NamespacedLocationFactory {

  /**
   * @return a Map of {@link Id.Namespace} to its {@link Location} on the filesystem
   */
  Map<Id.Namespace, Location> list() throws IOException;

  /**
   * Returns the base {@link Location} for the specified namespace on the filesystem
   *
   * @param namespaceId the namespace for which base location is desired
   * @return {@link Location} for the specified namespace on the filesystem
   */
  Location get(Id.Namespace namespaceId) throws IOException;

  /**
   * Returns a {@link Location} for the specified sub-path in the specified namespace
   *
   * @param namespaceId the namespace for which the {@link Location} is desired
   * @param subPath the sub-path under the base location of the specified namespace
   * @return {@link Location} for the specified sub-path in the specified namespace
   */
  Location get(Id.Namespace namespaceId, @Nullable String subPath) throws IOException;

  /**
   * Returns the base {@link Location} for all CDAP data. This location contains all
   * the namespace locations.
   */
  Location getBaseLocation() throws IOException;

}
