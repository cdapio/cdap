/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.proto.id.NamespacedArtifactId;

import java.util.Map;

/**
 * An interface to manage interactions with an {@link NamespacedArtifactId artifact} in tests.
 */
public interface ArtifactManager {
  /**
   * Write properties to the artifact.
   *
   * @param properties the properties to write
   */
  void writeProperties(Map<String, String> properties) throws Exception;

  /**
   * Remove properties from the artifact.
   */
  void removeProperties() throws Exception;

  /**
   * Delete the artifact
   */
  void delete() throws Exception;
}
