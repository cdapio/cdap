/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.logbuffer;

import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Represents a pending {@link LogBufferRequest} that is yet to be processed by log processor pipelines.
 */
public class PendingLogBufferRequest {
  private final LogBufferRequest originalRequest;
  private boolean completed;
  private Throwable failureCause;

  PendingLogBufferRequest(LogBufferRequest originalRequest) {
    this.originalRequest = originalRequest;
  }

  boolean isCompleted() {
    return completed;
  }

  boolean isSuccess() {
    if (!isCompleted()) {
      throw new IllegalStateException("Write is not yet completed");
    }
    return failureCause == null;
  }

  @Nullable
  Throwable getFailureCause() {
    return failureCause;
  }

  void completed(@Nullable Throwable failureCause) {
    completed = true;
    this.failureCause = failureCause;
  }

  public Iterator<byte[]> getIterator() {
    return originalRequest.iterator();
  }
}
