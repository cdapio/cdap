/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.store;

import com.google.inject.ImplementedBy;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Store for namespaces. This interface is by default implemented by {@link DefaultNamespaceStore}, unless
 * explicitly overridden for testing.
 */
@ImplementedBy(DefaultNamespaceStore.class)
public interface NamespaceStore {

  /**
   * Creates a new namespace.
   *
   * @param metadata {@link NamespaceMeta} representing the namespace metadata
   * @return existing {@link NamespaceMeta} if a namespace with the specified name existed already, null if the
   * a namespace with the specified name did not exist, and was created successfully
   * These semantics of return type are borrowed from {@link java.util.concurrent.ConcurrentHashMap#putIfAbsent}
   */
  @Nullable
  NamespaceMeta create(NamespaceMeta metadata);

  /**
   * Updates the namespace meta.
   *
   * @param metadata {@link NamespaceMeta} representing the namespace metadata
   */
  void update(NamespaceMeta metadata);

  /**
   * Retrieves a namespace from the namespace metadata store.
   *
   * @param id {@link NamespaceId} of the requested namespace
   * @return {@link NamespaceMeta} of the requested namespace
   */
  @Nullable
  NamespaceMeta get(NamespaceId id);

  /**
   * Deletes a namespace from the namespace metadata store.
   *
   * @param id {@link NamespaceId} of the namespace to delete
   * @return {@link NamespaceMeta} of the namespace if it was found and deleted, null if the specified namespace did not
   * exist
   * These semantics of return type are borrowed from {@link java.util.concurrent.ConcurrentHashMap#remove}
   */
  @Nullable
  NamespaceMeta delete(NamespaceId id);

  /**
   * Lists all registered namespaces.
   *
   * @return a list of all registered namespaces
   */
  List<NamespaceMeta> list();

  /**
   * Counts all namespaces.
   * @return long of the count
   */
  long getNamespaceCount();
}
