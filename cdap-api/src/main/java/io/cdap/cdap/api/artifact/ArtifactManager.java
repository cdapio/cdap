/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.api.artifact;

import io.cdap.cdap.api.annotation.Beta;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.Location;

/**
 * Provides access to artifacts
 */
@Beta
public interface ArtifactManager {
  Location getArtifactLocation(ArtifactSummary artifactSummary, @Nullable String namespace) throws IOException;

  /**
   * Get the list of artifacts in the repository in the current and system namespace
   *
   * @return list of artifact info
   * @throws IOException when there is an error retrieving artifacts
   */
  List<ArtifactInfo> listArtifacts() throws IOException;

  /**
   * Get the list of artifacts in the repository in the specified namespace and system namespace
   *
   * @param namespace the namespace to list artifacts for
   * @return list of artifact info
   * @throws IOException when there is an error retrieving artifacts
   */
  List<ArtifactInfo> listArtifacts(String namespace) throws IOException;

  /**
   * Create a class loader using the artifact represented by artifactInfo with parent as parentClassloader
   * Call to this method might take a long time based on the size of the artifact.
   * If called from short transactions, it is possible this call will timeout if the artifact size is large
   *
   * @param artifactInfo artifact info
   * @param parentClassLoader parent class loader, if null bootstrap classLoader shall be used as parent
   * @throws IOException if there were any exception while creating the class loader or if the artifact is not found
   * @return Closeable class loader, calling close on this does the necessary cleanup.
   */
  CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
                                         @Nullable ClassLoader parentClassLoader) throws IOException;

  /**
   * Create a class loader using the artifact represented by artifactInfo with parent as parentClassloader
   * Call to this method might take a long time based on the size of the artifact.
   * If called from short transactions, it is possible this call will timeout if the artifact size is large
   *
   * @param namespace the namespace of the specified artifact. This should be the same namespace that was used when
   *  calling {@link #listArtifacts(String)}
   * @param artifactInfo artifact info
   * @param parentClassLoader parent class loader, if null bootstrap classLoader shall be used as parent
   * @throws IOException if there were any exception while creating the class loader or if the artifact is not found
   * @return Closeable class loader, calling close on this does the necessary cleanup.
   */
  CloseableClassLoader createClassLoader(String namespace, ArtifactInfo artifactInfo,
                                         @Nullable ClassLoader parentClassLoader) throws IOException;
}
