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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Factory interface for creating {@link ArtifactManager} that binds to a giving namespace.
 * This interface is for Guice assisted binding, hence there will be no concrete implementation of it.
 */
public interface ArtifactManagerFactory {

  /**
   * Returns an implementation of {@link ArtifactManager} that operates on the given {@link NamespaceId}.
   *
   * @param namespaceId   the namespace that the {@link ArtifactManager} will be operating on
   * @param retryStrategy the {@link RetryStrategy} to use for dealing with retryable failures
   * @return a new instance of {@link ArtifactManager}.
   */
  ArtifactManager create(NamespaceId namespaceId, RetryStrategy retryStrategy);

  /**
   * Returns an implementation of {@link ArtifactManager} that operates on the given {@link NamespaceId}, uses the
   * provided {@link RetryStrategy} and uses the given {@link ArtifactLocalizerClient} to fetch artifact location.
   * Use this only if a {@link ArtifactLocalizerClient} is required to copy artifacts from a remote location.
   * @param namespaceId {@link NamespaceId} for the artifacts
   * @param retryStrategy {@link RetryStrategy} for ArtifactManager
   * @param artifactLocalizerClient {@link ArtifactLocalizerClient} for getting cached location of artifacts
   * @return
   */
  ArtifactManager create(NamespaceId namespaceId, RetryStrategy retryStrategy,
                         ArtifactLocalizerClient artifactLocalizerClient);
}
