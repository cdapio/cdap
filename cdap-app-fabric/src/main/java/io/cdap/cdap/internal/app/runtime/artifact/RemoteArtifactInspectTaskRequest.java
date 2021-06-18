/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.gateway.handlers.FileFetcherHttpHandlerInternal;
import io.cdap.cdap.internal.app.worker.TaskWorkerHttpHandlerInternal;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Encapsulates all parameters required by {@link RemoteArtifactInspectTask} to perform artifact inspection
 * remotely in {@link TaskWorkerHttpHandlerInternal}
 */
public class RemoteArtifactInspectTaskRequest {
  /**
   * The URI of artifact location that {@link RemoteArtifactInspectTask} will fetch the artifact from
   * via {@link FileFetcherHttpHandlerInternal}
   */
  private final URI artifactURI;
  private final List<ArtifactDetail> parentArtifacts;
  private final Set<PluginClass> additionalPlugins;

  public RemoteArtifactInspectTaskRequest(URI artifactURI, List<ArtifactDetail> artifactDetailList,
                                          Set<PluginClass> additionalPlugins) {
    this.artifactURI = artifactURI;
    this.parentArtifacts = artifactDetailList;
    this.additionalPlugins = additionalPlugins;
  }

  URI getArtifactURI() {
    return artifactURI;
  }

  List<ArtifactDetail> getParentArtifacts() {
    return parentArtifacts;
  }

  Set<PluginClass> getAdditionalPlugins() {
    return additionalPlugins;
  }
}

