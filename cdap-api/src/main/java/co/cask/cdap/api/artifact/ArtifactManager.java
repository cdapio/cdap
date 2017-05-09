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

import java.util.List;
import javax.annotation.Nullable;

/**
 * Provides access to artifacts
 */
public interface ArtifactManager {
  /**
   * Get the list of artifacts in the repository in the current namespace
   * @return
   */
  List<ArtifactInfo> listArtifacts();

  /**
   * Create a class loader using the artifact represented by artifactInfo with parent as parentClassloader.
   * @param artifactInfo artifact
   * @param parentClassLoader parent class loader, if null bootstrap classLoader shall be used as parent.
   * @return
   */
  ClassLoader createClassLoader(ArtifactInfo artifactInfo, @Nullable ClassLoader parentClassLoader);
}
