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
import io.cdap.cdap.internal.io.ExposedByteArrayOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * Represents a context for a {@link RunnableTask}.
 * This context is used for writing back the result of {@link RunnableTask} execution.
 */
public class RunnableTaskContext {
  private final ExposedByteArrayOutputStream outputStream;
  @Nullable
  private final String param;
  @Nullable
  private final RunnableTaskRequest embeddedRequest;
  @Nullable
  private final String namespace;
  @Nullable
  private final ArtifactId artifactId;
  @Nullable
  private final SystemAppTaskContext systemAppTaskContext;

  private boolean terminateOnComplete;

  public RunnableTaskContext(@Nullable String param, @Nullable RunnableTaskRequest embeddedRequest,
                             @Nullable String namespace, @Nullable ArtifactId artifactId,
                             @Nullable SystemAppTaskContext systemAppTaskContext) {
    this.param = param;
    this.embeddedRequest = embeddedRequest;
    this.namespace = namespace;
    this.artifactId = artifactId;
    this.systemAppTaskContext = systemAppTaskContext;
    this.outputStream = new ExposedByteArrayOutputStream();
    this.terminateOnComplete = true;
  }

  public void writeResult(byte[] data) throws IOException {
    if (outputStream.size() != 0) {
      outputStream.reset();
    }
    outputStream.write(data);
  }

  /**
   * Sets to terminate the task runner on the task completion
   *
   * @param terminate {@code true} to terminate the task runner, {@code false} to keep it running.
   */
  public void setTerminateOnComplete(boolean terminate) {
    this.terminateOnComplete = terminate;
  }

  /**
   * @return {@code true} if terminate the task runner after the task completed.
   */
  public boolean isTerminateOnComplete() {
    return terminateOnComplete;
  }

  public ByteBuffer getResult() {
    return outputStream.toByteBuffer();
  }

  @Nullable
  public String getParam() {
    return param;
  }

  @Nullable
  public RunnableTaskRequest getEmbeddedRequest() {
    return embeddedRequest;
  }

  @Nullable
  public String getNamespace() {
    return namespace;
  }

  @Nullable
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  @Nullable
  public SystemAppTaskContext getRunnableTaskSystemAppContext() {
    return systemAppTaskContext;
  }

  public static Builder getBuilder() {
    return new Builder();
  }

  /**
   * Builder for RunnableTaskContext
   */
  public static class Builder {
    private RunnableTaskParam param;
    @Nullable
    private String namespace;
    @Nullable
    private ArtifactId artifactId;
    @Nullable
    private SystemAppTaskContext systemAppTaskContext;

    private Builder() {

    }

    public Builder withParam(RunnableTaskParam param) {
      this.param = param;
      return this;
    }

    public Builder withNamespace(@Nullable String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder withArtifactId(@Nullable ArtifactId artifactId) {
      this.artifactId = artifactId;
      return this;
    }

    public Builder withTaskSystemAppContext(@Nullable SystemAppTaskContext
                                              systemAppTaskContext) {
      this.systemAppTaskContext = systemAppTaskContext;
      return this;
    }

    public RunnableTaskContext build() {
      return new RunnableTaskContext(param.getSimpleParam(), param.getEmbeddedTaskRequest(), namespace, artifactId,
                                     systemAppTaskContext);
    }
  }
}
