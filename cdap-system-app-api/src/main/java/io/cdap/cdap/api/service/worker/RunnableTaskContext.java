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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Represents a context for a {@link RunnableTask}. This context is used for writing back the result
 * of {@link RunnableTask} execution.
 */
public class RunnableTaskContext {

  private static final Logger LOG = LoggerFactory.getLogger(RunnableTaskContext.class);

  private final String className;
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

  private final AtomicBoolean cleanupTaskExecuted;

  private boolean terminateOnComplete;

  private Runnable cleanupTask;

  public RunnableTaskContext(RunnableTaskRequest taskRequest) {
    this(taskRequest, null);
  }

  public RunnableTaskContext(RunnableTaskRequest taskRequest,
      @Nullable SystemAppTaskContext systemAppTaskContext) {
    Optional<RunnableTaskParam> taskParam = Optional.ofNullable(taskRequest.getParam());

    this.className = taskRequest.getClassName();
    this.param = taskParam.map(RunnableTaskParam::getSimpleParam).orElse(null);
    this.embeddedRequest = taskParam.map(RunnableTaskParam::getEmbeddedTaskRequest).orElse(null);
    this.namespace = taskRequest.getNamespace();
    this.artifactId = taskRequest.getArtifactId();
    this.systemAppTaskContext = systemAppTaskContext;
    this.terminateOnComplete = true;
    this.outputStream = new ExposedByteArrayOutputStream();
    this.cleanupTaskExecuted = new AtomicBoolean(false);
  }

  public void writeResult(byte[] data) throws IOException {
    if (outputStream.size() != 0) {
      outputStream.reset();
    }
    outputStream.write(data);
  }

  public void setCleanupTask(Runnable cleanupTask) {
    this.cleanupTask = cleanupTask;
  }

  public void executeCleanupTask() {
    if (cleanupTaskExecuted.compareAndSet(false, true)) {
      try {
        Optional.ofNullable(cleanupTask).ifPresent(Runnable::run);
      } catch (Exception e) {
        LOG.warn("Exception raised when executing cleanup task", e);
      }
    }
  }

  /**
   * Sets to terminate the task runner on the task completion
   *
   * @param terminate {@code true} to terminate the task runner, {@code false} to keep it
   *     running.
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

  public String getClassName() {
    return className;
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

}
