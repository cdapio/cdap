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

package io.cdap.cdap.internal.app.worker;

/**
 * Request for launching a runnable task.
 */
public class RunnableTaskRequest {
  private final String className;
  private final String param;

  /**
   * If true, and for security reasons, task worker pod will be killed once the request is performed by worker pod.
   */
  private final boolean hasUserCode;

  public RunnableTaskRequest(String className, String param) {
    this.className = className;
    this.param = param;
    hasUserCode = true;
  }

  public RunnableTaskRequest(String className, String param, boolean hasUserCode) {
    this.className = className;
    this.param = param;
    this.hasUserCode = hasUserCode;
  }

  public String getClassName() {
    return className;
  }

  public String getParam() {
    return param;
  }

  public boolean hasUserCode() {
    return hasUserCode;
  }
}
