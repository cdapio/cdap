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
  private final String param;
  @Nullable
  private ArtifactId artifactId;
  @Nullable
  private String namespace;
  @Nullable
  private RunnableTaskRequest delegate;

  private RunnableTaskRequest(String className, String param,
                              @Nullable ArtifactId artifactId, @Nullable String namespace,
                              @Nullable RunnableTaskRequest delegate) {
    this.className = className;
    this.param = param;
    this.artifactId = artifactId;
    this.namespace = namespace;
    this.delegate = delegate;
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

  @Nullable
  public RunnableTaskRequest getDelegate() {
    return delegate;
  }

  @Override
  public String toString() {
    String requestString = "RunnableTaskRequest{className=%s, param=%s, artifactId=%s, " +
      "namespace=%s, module=%s, delegate=%s}";
    return String.format(requestString, className, param, artifactId, namespace, delegate);
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
    private String param;
    @Nullable
    private ArtifactId artifactId;
    @Nullable
    private String namespace;
    @Nullable
    private RunnableTaskRequest delegate;

    private Builder(String className) {
      this.className = className;
    }

    public Builder withParam(String param) {
      this.param = param;
      return this;
    }

    public Builder withModule(String module) {
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

    public Builder withDelegate(RunnableTaskRequest delegate) {
      this.delegate = delegate;
      return this;
    }

    public RunnableTaskRequest build() {
      return new RunnableTaskRequest(className, param, artifactId, namespace, delegate);
    }
  }
}
