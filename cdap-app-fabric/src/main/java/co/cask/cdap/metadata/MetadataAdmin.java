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

package co.cask.cdap.metadata;

import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;

import java.util.Map;

/**
 * Interface to interact with Metadata.
 */
public interface MetadataAdmin {

  /**
   * Adds the specified {@link Map} to the business metadata of the specified {@link Id.Application},
   * {@link Id.Program}, {@link Id.DatasetInstance} or {@link Id.Stream}.
   * Existing keys are updated with new values, newer keys are appended to the metadata.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void add(Id.NamespacedId entityId, Map<String, String> metadata) throws NotFoundException;

  /**
   * Adds the specified tags to specified {@link Id.Application}, {@link Id.Program}, {@link Id.DatasetInstance} or
   * {@link Id.Stream}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void addTags(Id.NamespacedId entityId, String... tags) throws NotFoundException;

  /**
   * @return a {@link Map} representing the business metadata of the specified {@link Id.Application},
   * {@link Id.Program}, {@link Id.DatasetInstance} or {@link Id.Stream}
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  Map<String, String> get(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * @return all the tags for the specified {@link Id.Application}, {@link Id.Program}, {@link Id.DatasetInstance} or
   * {@link Id.Stream}
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  Iterable<String> getTags(Id.NamespacedId entityId) throws NotFoundException;

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.Application}, {@link Id.Program},
   * {@link Id.DatasetInstance} or {@link Id.Stream}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void remove(Id.NamespacedId appId, String ... keys) throws NotFoundException;

  /**
   * Removes the specified tags from the specified {@link Id.Application}, {@link Id.Program},
   * {@link Id.DatasetInstance} or {@link Id.Stream}.
   *
   * @throws NamespaceNotFoundException if the namespace is not found
   * @throws ApplicationNotFoundException if the application is not found
   */
  void removeTags(Id.NamespacedId appId, String ... tags) throws NotFoundException;
}
