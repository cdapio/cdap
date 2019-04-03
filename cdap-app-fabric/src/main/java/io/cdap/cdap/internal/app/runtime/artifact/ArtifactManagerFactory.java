/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.proto.id.NamespaceId;

/**
 * Factory interface for creating {@link ArtifactManager} that binds to a giving namespace.
 * This interface is for Guice assisted binding, hence there will be no concrete implementation of it.
 */
public interface ArtifactManagerFactory {

  /**
   * Returns an implementation of {@link ArtifactManager} that operates on the given {@link NamespaceId}.
   *
   * @param namespaceId the namespace that the {@link ArtifactManager} will be operating on
   * @param retryStrategy the {@link RetryStrategy} to use for dealing with retryable failures
   * @return a new instance of {@link ArtifactManager}.
   */
  ArtifactManager create(NamespaceId namespaceId, RetryStrategy retryStrategy);
}
