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
  @Nullable
  private final RunnableTaskParam param;
  @Nullable
  private final ArtifactId artifactId;
  @Nullable
  private final String namespace;
  @Nullable

  private RunnableTaskRequest(String className, @Nullable String param,
                              @Nullable ArtifactId artifactId, @Nullable String namespace,
                              @Nullable RunnableTaskRequest embeddedTaskRequest) {
    this.className = className;
    this.param = new RunnableTaskParam(param, embeddedTaskRequest);
    this.artifactId = artifactId;
    this.namespace = namespace;
  }

  public String getClassName() {
    return className;
  }

  @Nullable
  public RunnableTaskParam getParam() {
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
    String requestString =
      "RunnableTaskRequest{className=%s, param=%s, artifactId=%s, namespace=%s}";
    return String.format(requestString, className, param == null ? null :
                           param.getSimpleParam().length() > 500 ?
                             param.getSimpleParam().substring(500) : param.getSimpleParam(),
                         artifactId, namespace);
  }

  public static Builder getBuilder(String taskClassName) {
    return new Builder(taskClassName);
  }

  /**
   * Builder for RunnableTaskRequest
   */
  public static class Builder {
    private final String className;
    @Nullable
    private String simpleParam;
    @Nullable
    private ArtifactId artifactId;
    @Nullable
    private String namespace;
    @Nullable
    private RunnableTaskRequest embeddedTaskRequest;

    private Builder(String className) {
      this.className = className;
    }

    public Builder withParam(String param) {
      this.simpleParam = param;
      return this;
    }

    public Builder withNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder withArtifact(ArtifactId artifactId) {
      this.artifactId = artifactId;
      return this;
    }

    public Builder withEmbeddedTaskRequest(RunnableTaskRequest embeddedTaskRequest) {
      this.embeddedTaskRequest = embeddedTaskRequest;
      return this;
    }

    public RunnableTaskRequest build() {
      return new RunnableTaskRequest(className, simpleParam, artifactId, namespace, embeddedTaskRequest);
    }
  }
}
