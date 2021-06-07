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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * Represents a context for a {@link RunnableTask}.
 * This context is used for writing back the result of {@link RunnableTask} execution.
 */
public class RunnableTaskContext {
  private final ByteArrayOutputStream outputStream;
  private final String param;
  private final URI fileURI;
  @Nullable
  private final String namespace;
  @Nullable
  private final ArtifactId artifactId;
  @Nullable
  private final SystemAppTaskContext systemAppTaskContext;

  public RunnableTaskContext(String param, @Nullable URI fileURI, @Nullable String namespace,
                             @Nullable ArtifactId artifactId, @Nullable SystemAppTaskContext systemAppTaskContext) {
    this.param = param;
    this.fileURI = fileURI;
    this.namespace = namespace;
    this.artifactId = artifactId;
    this.systemAppTaskContext = systemAppTaskContext;
    this.outputStream = new ByteArrayOutputStream();
  }

  public void writeResult(byte[] data) throws IOException {
    outputStream.write(data);
  }

  public byte[] getResult() {
    return outputStream.toByteArray();
  }

  public String getParam() {
    return param;
  }

  public URI getFileURI() {
    return fileURI;
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
    private String param;
    @Nullable
    private URI fileURI;
    @Nullable
    private String namespace;
    @Nullable
    private ArtifactId artifactId;
    @Nullable
    private SystemAppTaskContext systemAppTaskContext;

    private Builder() {

    }

    public Builder withParam(String param) {
      this.param = param;
      return this;
    }

    public Builder withFileURI(@Nullable URI fileURI) {
      this.fileURI = fileURI;
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
      return new RunnableTaskContext(param, fileURI, namespace, artifactId, systemAppTaskContext);
    }
  }
}
