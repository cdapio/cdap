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

package io.cdap.cdap.store;

import com.google.inject.ImplementedBy;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryMeta;

/**
 * Store for repository configuration. This interface is by default implemented by {@link DefaultRepositoryStore},
 * unless explicitly overridden for testing.
 */
@ImplementedBy(DefaultRepositoryStore.class)
public interface RepositoryStore {
  /**
   * Sets the repository configuration foe the namespace.
   * @param namespace {@link NamespaceId} The namespace where the repository belongs
   * @param repository The {@link RepositoryConfig} that is to be set in table
   * @return {@link RepositoryMeta}
   */
  RepositoryMeta setRepository(NamespaceId namespace, RepositoryConfig repository) throws NamespaceNotFoundException;

  /**
   * Deletes the repository configuration of the namespace.
   * @param namespace {@link NamespaceId} The namespace of the repository to delete
   */
  void deleteRepository(NamespaceId namespace);

  /**
   * 
   * @param namespace {@link NamespaceId} The namespace of the repository to get
   * @return {@link RepositoryMeta}
   */
  RepositoryMeta getRepositoryMeta(NamespaceId namespace) throws RepositoryNotFoundException;
}
