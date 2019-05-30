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

package io.cdap.cdap.store.artifact.datafetchers;

import com.google.inject.Inject;
import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.store.artifact.schema.ArtifactFields;

/**
 * TODO
 */
public class ArtifactDetailDataFetcher {

  private final ArtifactRepository artifactRepository;

  /**
   * TODO
   */
  @Inject
  public ArtifactDetailDataFetcher(ArtifactRepository artifactRepository) {
    this.artifactRepository = artifactRepository;
  }

  /**
   * TODO
   */
  public DataFetcher getArtifactDetailDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        String namespace = dataFetchingEnvironment.getArgument(ArtifactFields.NAMESPACE);
        String name = dataFetchingEnvironment.getArgument(ArtifactFields.NAME);
        String version = dataFetchingEnvironment.getArgument(ArtifactFields.VERSION);
        ArtifactDetail artifactDetail = artifactRepository
          .getArtifact(Id.Artifact.from(Id.Namespace.from(namespace), name, version));

        return artifactDetail.getDescriptor();
      }
    );
  }

}
