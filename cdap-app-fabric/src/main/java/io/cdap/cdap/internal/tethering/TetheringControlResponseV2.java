/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import java.util.List;

/**
 * Response sent to tethering agent.
 */
public class TetheringControlResponseV2 {

  // Control messages sent by server.
  private final List<TetheringControlMessageWithId> controlMessages;
  // Server's tethering status
  private final TetheringStatus tetheringStatus;

  public TetheringControlResponseV2(List<TetheringControlMessageWithId> controlMessages,
      TetheringStatus tetheringStatus) {
    this.controlMessages = controlMessages;
    this.tetheringStatus = tetheringStatus;
  }

  public List<TetheringControlMessageWithId> getControlMessages() {
    return controlMessages;
  }

  public TetheringStatus getTetheringStatus() {
    return tetheringStatus;
  }
}
