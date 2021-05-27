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
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.id.Id;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Inspects a jar file to determine metadata about the artifact.
 */
interface ArtifactInspector {
  /**
   * Inspect the given artifact to determine the classes contained in the artifact.
   *
   * @param artifactId the id of the artifact to inspect
   * @param artifactFile the artifact file
   * @param parentClassLoader the parent classloader to use when inspecting plugins contained in the artifact.
   * For example, a ProgramClassLoader created from the artifact the input artifact extends
   * @param additionalPlugins Additional plugin classes
   * @return metadata about the classes contained in the artifact
   * @throws IOException if there was an exception opening the jar file
   * @throws InvalidArtifactException if the artifact is invalid. For example, if the application main class is not
   * actually an Application.
   */
  ArtifactClassesWithMetadata inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                              @Nullable ClassLoader parentClassLoader,
                                              Set<PluginClass> additionalPlugins)
    throws IOException, InvalidArtifactException;
}
