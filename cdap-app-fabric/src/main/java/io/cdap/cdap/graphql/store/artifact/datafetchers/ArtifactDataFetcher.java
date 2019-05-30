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
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.graphql.objects.Artifact;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

/**
 * Fetchers to get artifacts
 */
public class ArtifactDataFetcher {

  private final ArtifactStore artifactStore;

  @Inject
  ArtifactDataFetcher(ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
  }

  /**
   * Fetcher to get a list of artifacts
   *
   * @return the data fetcher
   */
  public DataFetcher getArtifactsDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        String namespace = dataFetchingEnvironment.getArgument(ArtifactFields.NAMESPACE);
        List<ArtifactDetail> artifactDetails = this.artifactStore.getArtifacts(new NamespaceId(namespace));

        List<Artifact> artifacts = new ArrayList<>();

        for (ArtifactDetail artifactDetail : artifactDetails) {
          Artifact artifact = getArtifact(artifactDetail, namespace);
          artifacts.add(artifact);
        }

        return artifacts;
      }
    );
  }

  /**
   * Fetcher to get an artifact
   *
   * @return the data fetcher
   */
  public DataFetcher getArtifactDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        String namespace = dataFetchingEnvironment.getArgument(ArtifactFields.NAMESPACE);
        String name = dataFetchingEnvironment.getArgument(ArtifactFields.NAME);
        String version = dataFetchingEnvironment.getArgument(ArtifactFields.VERSION);

        Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.from(namespace), name, version);
        ArtifactDetail artifactDetail = this.artifactStore.getArtifact(artifactId);

        return getArtifact(artifactDetail, namespace);
      }
    );
  }

  private Artifact getArtifact(ArtifactDetail artifactDetail, String namespace) {
    ArtifactDescriptor artifactDescriptor = artifactDetail.getDescriptor();
    ArtifactId artifactId = artifactDescriptor.getArtifactId();

    return new Artifact.Builder()
      .name(artifactId.getName())
      .version(artifactId.getVersion().getVersion())
      .scope(artifactId.getScope().toString())
      .namespace(namespace)
      .location(artifactDescriptor.getLocation())
      .meta(artifactDetail.getMeta())
      .build();
  }

}
