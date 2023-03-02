/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.program;

import com.google.gson.Gson;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ArtifactId;
import java.util.Map;

/**
 * Publishing program state messages and heartbeats
 */
public interface ProgramStatePublisher {

  /**
   * @return if program status notification with given system arguments would need a program start
   *     (false) or not (true). If true it means that program start is handled elsewhere (e.g. in
   *     preview or workflow) and notification is only to report program start already happened.
   *     Such notification would not need to have the {@link ProgramOptionConstants#PROGRAM_DESCRIPTOR}
   *     field and will have {@link ProgramOptionConstants#PROGRAM_ARTIFACT_ID} field instead.
   */
  static boolean isProgramStartSkipped(Map<String, String> systemArguments) {
    boolean isInWorkflow = systemArguments.containsKey(ProgramOptionConstants.WORKFLOW_NAME);
    boolean skipProvisioning = Boolean.parseBoolean(
        systemArguments.get(ProgramOptionConstants.SKIP_PROVISIONING));

    return isInWorkflow || skipProvisioning;
  }

  /**
   * @return {@link ArtifactId} retrieved from either {@link ProgramOptionConstants#PROGRAM_ARTIFACT_ID}
   *     or {@link ProgramOptionConstants#PROGRAM_DESCRIPTOR}
   */
  static ArtifactId getArtifactId(Gson gson, Map<String, String> properties) {
    if (properties.containsKey(ProgramOptionConstants.PROGRAM_ARTIFACT_ID)) {
      return gson.fromJson(properties.get(ProgramOptionConstants.PROGRAM_ARTIFACT_ID),
          ArtifactId.class);
    } else {
      //Old program notification, it is passing program descriptor
      return gson.fromJson(properties.get(ProgramOptionConstants.PROGRAM_DESCRIPTOR),
              ProgramDescriptor.class)
          .getArtifactId();
    }
  }

  /**
   * Publish message which is identified by notificationType and the properties.
   *
   * @param notificationType type of the notification of the message
   * @param properties properties of the message to publish
   */
  void publish(Notification.Type notificationType, Map<String, String> properties);
}
