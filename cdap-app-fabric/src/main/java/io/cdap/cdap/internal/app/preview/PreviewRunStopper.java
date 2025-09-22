/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.proto.id.ApplicationId;

/**
 * Interface to kill the preview runner service.
 */
public interface PreviewRunStopper {

  /**
   * Stops the preview as identified by the application id.
   *
   * @param preview id of the preview application to be stopped
   * @throws Exception if any error while stopping
   */
  void stop(ApplicationId preview) throws Exception;

  /**
   * Stops the preview runner associated with the given poller information.
   * <p>
   * In a distributed environment (e.g., Kubernetes), this method is responsible for finding the
   * runner's container and terminating it. This is a critical action for enforcing the
   * "one-request-per-runner" security policy when a suspicious runner is detected.
   * </p><p>
   * In a non-distributed (standalone) environment, this is a no-op as there is no separate container
   * to terminate.
   * </p>
   *
   * @param pollerInfo The poller information identifying the preview runner to be stopped.
   * @throws Exception if there is an error while attempting to stop the runner in a distributed
   *                   environment.
   */
  void stop(byte[] pollerInfo) throws Exception;
}
