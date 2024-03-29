/*
 * Copyright © 2015 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Thrown when an artifact does not exist.
 */
public class ArtifactNotFoundException extends NotFoundException {

  public ArtifactNotFoundException(String namespace, String name) {
    super("artifact", namespace + ":" + name);
  }

  public ArtifactNotFoundException(NamespaceId namespace, String name) {
    this(namespace.getNamespace(), name);
  }

  public ArtifactNotFoundException(ArtifactId artifactId) {
    super("artifact", artifactId.toString());
  }

  public ArtifactNotFoundException(String namespace, String name, String version) {
    super("artifact", namespace + ":" + name + ' ' + version);
  }
}
