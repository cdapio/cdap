/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Thrown when a plugin does not exist.
 */
public class PluginNotExistsException extends NotFoundException {

  @Deprecated
  public PluginNotExistsException(Id.Namespace namespace, String type, String name) {
    this(namespace.toEntityId(), type, name);
  }

  @Deprecated
  public PluginNotExistsException(Id.Artifact artifactId, String type, String name) {
    this(artifactId.toEntityId(), type, name);
  }

  public PluginNotExistsException(NamespaceId namespace, String type, String name) {
    super("plugin", String.format("%s:%s:%s", namespace.getNamespace(), type, name));
  }

  public PluginNotExistsException(ArtifactId artifactId, String type, String name) {
    super("plugin", String.format("%s:%s:%s:%s:%s",
        artifactId.getNamespace(), type, name, artifactId.getArtifact(),
        artifactId.getVersion()));
  }
}
