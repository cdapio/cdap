/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common;

import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Interface to fetch artifact metadata
 */
public interface ArtifactRepositoryReader {


  /**
   * Get all artifacts in the given namespace, optionally including system artifacts as well. Will
   * never return null. If no artifacts exist, an empty list is returned. Namespace existence is not
   * checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param includeSystem whether system artifacts should be included in the results
   * @return an unmodifiable list of artifacts that belong to the given namespace
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, boolean includeSystem)
      throws Exception;

  /**
   * Get all artifacts in the given namespace of the given name. Will never return null. If no
   * artifacts exist, an exception is thrown. Namespace existence is not checked.
   *
   * @param namespace the namespace to get artifacts from
   * @param name the name of artifacts to get
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if no artifacts of the given name in the given namespace
   *     exist
   */
  List<ArtifactSummary> getArtifactSummaries(NamespaceId namespace, String name, int limit,
      ArtifactSortOrder order) throws Exception;

  /**
   * Get all artifacts in the given artifact range. Will never return null.
   *
   * @param range the range of the artifact
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of artifacts in the given namespace of the given name
   * @throws IOException if there as an exception reading from the meta store
   */
  List<ArtifactSummary> getArtifactSummaries(ArtifactRange range, int limit,
      ArtifactSortOrder order) throws Exception;


  /**
   * Get details about the given artifact. Will never return null. If no such artifact exist, an
   * exception is thrown. Namespace existence is not checked.
   *
   * @param artifactId the id of the artifact to get
   * @return details about the given artifact
   * @throws IOException if there as an exception reading from the meta store
   * @throws ArtifactNotFoundException if the given artifact does not exist
   */
  ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception;

  /**
   * Returns an input stream for reading the artifact bytes. If no such artifact exists, or an error
   * occurs during reading, an exception is thrown.
   *
   * @param artifactId the id of the artifact to get
   * @return an InputStream for the artifact bytes
   * @throws IOException if there as an exception reading from the store.
   * @throws NotFoundException if the given artifact does not exist
   */
  InputStream newInputStream(Id.Artifact artifactId) throws IOException, NotFoundException;

  /**
   * Get all artifact details that match artifacts in the given ranges.
   *
   * @param range the range to match artifacts in
   * @param limit the limit number of the result
   * @param order the order of the result
   * @return an unmodifiable list of all artifacts that match the given ranges. If none exist, an
   *     empty list is returned
   */
  List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit, ArtifactSortOrder order)
      throws Exception;
}
