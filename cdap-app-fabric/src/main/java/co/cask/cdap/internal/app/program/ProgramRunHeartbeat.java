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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.proto.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Heartbeat thread to publish heart beat messages with notification type heart_beat
 */
public class ProgramRunHeartbeat implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunHeartbeat.class);

  private final ProgramStatePublisher programStatePublisher;
  private final Map<String, String> properties;

  ProgramRunHeartbeat(ProgramStatePublisher programStatePublisher, Map<String, String> properties) {
    this.programStatePublisher = programStatePublisher;
    this.properties = properties;
  }

  @Override
  public void run() {
    programStatePublisher.publish(Notification.Type.HEART_BEAT, properties);
  }
}
