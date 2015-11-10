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

package co.cask.cdap.proto;

import javax.annotation.Nullable;

/**
 * Describes the instances of a program, as returned by the batch instances endpoint
 * POST /namespaces/{namespace}/instances.
 */
public class BatchRunnableInstances extends BatchRunnable {
  private final int statusCode;
  private final Integer provisioned;
  private final Integer requested;
  private final String error;

  public BatchRunnableInstances(BatchRunnable runnable, int statusCode, int provisioned, int requested) {
    super(runnable.appId, runnable.programType, runnable.programId, runnable.runnableId);
    this.statusCode = statusCode;
    this.provisioned = provisioned;
    this.requested = requested;
    this.error = null;
  }

  public BatchRunnableInstances(BatchRunnable runnable, int statusCode, String error) {
    super(runnable.appId, runnable.programType, runnable.programId, runnable.runnableId);
    this.statusCode = statusCode;
    this.provisioned = null;
    this.requested = null;
    this.error = error;
  }

  public int getStatusCode() {
    return statusCode;
  }

  /**
   * @return the number of provisioned instances, or null if there was an error getting instances.
   */
  @Nullable
  public Integer getProvisioned() {
    return provisioned;
  }

  /**
   * @return the number of requested instances, or null if there was an error getting instances.
   */
  @Nullable
  public Integer getRequested() {
    return requested;
  }

  /**
   * @return the error message, or null if there was no error.
   */
  @Nullable
  public String getError() {
    return error;
  }
}
