/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.api;

import io.cdap.cdap.api.security.AccessException;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

/**
 * Represents a basic resource manager with get, create, update, and delete functionality.ß
 * @param <R> The type of resource stored.
 */
public interface NamespaceResourceManager<R> {

  /**
   * Fetches a resource referred to by a reference.
   *
   * @param ref The namespace reference.
   * @return An optional containing the fetched resource.
   * @throws AccessException If the caller lacks permissions.
   * @throws IllegalArgumentException If the namespace resource reference is invalid.
   * @throws IOException If any transport errors occur.
   */
  Optional<R> get(NamespaceResourceReference ref)
      throws AccessException, IllegalArgumentException, IOException;

  /**
   * Lists names of all resources within a namespace.
   *
   * @param namespace The namespace to list resources in.
   * @return A collection of namespace resource references.
   * @throws AccessException If the caller lacks permissions.
   * @throws IOException If any transport errors occur.
   */
  Collection<NamespaceResourceReference> list(String namespace) throws AccessException, IOException;

  /**
   * Creates a resource.
   *
   * @param ref The reference to the resource.
   * @param obj The resource.
   * @throws AccessException If the caller lacks permissions.
   * @throws IOException If any transport errors occur.
   * @throws IllegalArgumentException If the provided resource is invalid.
   * @throws NamespaceResourceAlreadyExistsException If the resource already exists.
   */
  void create(NamespaceResourceReference ref, R obj) throws AccessException, IOException,
      IllegalArgumentException, NamespaceResourceAlreadyExistsException;

  /**
   * Updates an existing resource.
   *
   * @param ref The reference to the resource.
   * @param obj The resource.
   * @throws AccessException If the caller lacks permissions.
   * @throws IOException If any transport errors occur.
   * @throws IllegalArgumentException If the provided namespace resource or reference is invalid.
   * @throws NamespaceResourceNotFoundException If the resource does not exist.
   */
  void update(NamespaceResourceReference ref, R obj) throws AccessException, IOException,
      IllegalArgumentException, IllegalStateException, NamespaceResourceNotFoundException;

  /**
   * Deletes an existing resource.
   *
   * @param ref The reference to the resource.
   * @throws AccessException If the caller lacks permissions.
   * @throws IOException If any transport errors occur.
   * @throws IllegalArgumentException If the provided namespace resource reference is invalid.
   * @throws IllegalStateException If the resource cannot be deleted at this time.
   * @throws NamespaceResourceNotFoundException If the resource does not exist.
   */
  void delete(NamespaceResourceReference ref) throws AccessException, IOException,
      IllegalArgumentException, IllegalStateException, NamespaceResourceNotFoundException;
}
