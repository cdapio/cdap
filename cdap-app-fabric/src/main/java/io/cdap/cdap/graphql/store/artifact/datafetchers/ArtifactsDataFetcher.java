/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.artifact.datafetchers;

import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.graphql.objects.Artifact;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;

/**
 * TODO
 */
public class ArtifactsDataFetcher {

  private final ArtifactStore artifactStore;

  /**
   * TODO
   */
  @Inject
  ArtifactsDataFetcher(ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
  }

  /**
   * TODO how would we get a single artifact
   */
  public DataFetcher getArtifactsDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        String namespace = dataFetchingEnvironment.getArgument(ArtifactFields.NAMESPACE);
        List<ArtifactDetail> artifactDetails = this.artifactStore.getArtifacts(new NamespaceId(namespace));

        List<Artifact> artifacts = new ArrayList<>();

        for (ArtifactDetail artifactDetail : artifactDetails) {
          ArtifactDescriptor artifactDescriptor = artifactDetail.getDescriptor();
          ArtifactId artifactId = artifactDescriptor.getArtifactId();

          Artifact artifact = new Artifact.Builder()
            .name(artifactId.getName())
            .version(artifactId.getVersion().getVersion())
            .scope(artifactId.getScope().toString())
            .namespace(namespace)
            .location(artifactDescriptor.getLocation())
            .meta(artifactDetail.getMeta())
            .build();

          artifacts.add(artifact);
        }

        return artifacts;
      }
    );
  }

}
