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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Represents a context for a {@link RunnableTask}.
 * This context is used for writing back the result of {@link RunnableTask} execution.
 */
public class RunnableTaskContext {
  private final ByteArrayOutputStream outputStream;
  @Nullable
  private final String param;
  @Nullable
  private final RunnableTaskRequest delegateTaskRequest;
  @Nullable
  private final ClassLoader artifactClassLoader;

  private RunnableTaskContext(@Nullable String param, @Nullable ClassLoader artifactClassLoader,
                              @Nullable RunnableTaskRequest delegateTaskRequest) {
    this.param = param;
    this.artifactClassLoader = artifactClassLoader;
    this.delegateTaskRequest = delegateTaskRequest;
    this.outputStream = new ByteArrayOutputStream();
  }

  public void writeResult(byte[] data) throws IOException {
    outputStream.write(data);
  }

  public byte[] getResult() {
    return outputStream.toByteArray();
  }

  @Nullable
  public String getParam() {
    return param;
  }

  @Nullable
  public ClassLoader getArtifactClassLoader() {
    return artifactClassLoader;
  }

  public static Builder getBuilder() {
    return new Builder();
  }

  @Nullable
  public RunnableTaskRequest getDelegateTaskRequest() {
    return delegateTaskRequest;
  }

  /**
   * Builder for RunnableTaskContext
   */
  public static class Builder {
    @Nullable
    private String param;
    @Nullable
    private RunnableTaskRequest delegateTaskRequest;
    @Nullable
    private ClassLoader artifactClassLoader;

    private Builder() {

    }

    public Builder withParam(@Nullable String param) {
      this.param = param;
      return this;
    }

    public Builder withArtifactClassLoader(@Nullable ClassLoader artifactClassLoader) {
      this.artifactClassLoader = artifactClassLoader;
      return this;
    }

    public Builder withDelegateTaskRequest(@Nullable RunnableTaskRequest delegateTaskRequest) {
      this.delegateTaskRequest = delegateTaskRequest;
      return this;
    }

    public RunnableTaskContext build() {
      return new RunnableTaskContext(param, artifactClassLoader, delegateTaskRequest);
    }
  }
}
