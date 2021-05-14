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

package io.cdap.cdap.api.service.worker;


import io.cdap.cdap.api.artifact.ArtifactId;

import javax.annotation.Nullable;

/**
 * Request for launching a runnable task.
 */
public class RunnableTaskRequest {
  private final String className;
  private final String param;
  @Nullable
  private ArtifactId artifactId;
  @Nullable
  private String namespace;

  public RunnableTaskRequest(String className, String param) {
    this(className, param, null, null);
  }

  public RunnableTaskRequest(String className, String param,
                             @Nullable ArtifactId artifactId, @Nullable String namespace) {
    this.className = className;
    this.param = param;
    this.artifactId = artifactId;
    this.namespace = namespace;
  }

  public String getClassName() {
    return className;
  }

  public String getParam() {
    return param;
  }

  @Nullable
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  @Nullable
  public String getNamespace() {
    return namespace;
  }

  @Override
  public String toString() {
    String requestString = "RunnableTaskRequest{className=%s, param=%s, artifactId=%s, namespace=%s}";
    return String.format(requestString, className, param, artifactId, namespace);
  }
}
