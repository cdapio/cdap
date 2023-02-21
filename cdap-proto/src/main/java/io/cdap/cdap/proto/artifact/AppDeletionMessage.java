/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.cdap.proto.artifact;

import io.cdap.cdap.proto.id.ApplicationId;

/**
 * A container for messages in the app deletion topic.
 * It carries the app-id of the app to be deleted
 */
public final class AppDeletionMessage {
  private final ApplicationId applicationId;

  /**
   * Creates an instance for an entity.
   *
   * @param applicationId  the applicationId of the app to be deleted
   */
  public AppDeletionMessage(ApplicationId applicationId) {
    this.applicationId = applicationId;
  }

  /**
   * Returns the applicationId of the app to be deleted
   */
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Override
  public String toString() {
    return "AppDeletionMessage{" +
      "applicationId=" + applicationId +
      '}';
  }
}

