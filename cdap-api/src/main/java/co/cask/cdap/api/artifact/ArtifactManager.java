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

package co.cask.cdap.api.artifact;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Provides access to artifacts
 */
public interface ArtifactManager {
  /**
   * Get the list of artifacts in the repository in the current namespace
   * @return {@link List<ArtifactInfo>} list of artifact info
   * @throws IOException when there is an error retrieving artifacts
   */
  List<ArtifactInfo> listArtifacts() throws IOException;

  /**
   * Create a class loader using the artifact represented by artifactInfo with parent as parentClassloader.
   * @param artifactInfo artifact info
   * @param parentClassLoader parent class loader, if null bootstrap classLoader shall be used as parent.
   * @throws Exception if there were any exception while creating a class loader
   * @return Closeable class loader, calling close on this does the necessary cleanup.
   */
  CloseableClassLoader createClassLoader(ArtifactInfo artifactInfo,
                                         @Nullable ClassLoader parentClassLoader) throws Exception;
}
