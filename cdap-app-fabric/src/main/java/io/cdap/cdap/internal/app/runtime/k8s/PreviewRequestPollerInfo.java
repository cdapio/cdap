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

package io.cdap.cdap.internal.app.runtime.k8s;

import javax.annotation.Nullable;

/**
 * Poller information holder.
 */
public class PreviewRequestPollerInfo {

  private final int instanceId;
  private final String instanceUid;

  public PreviewRequestPollerInfo(int instanceId, @Nullable String instanceUid) {
    this.instanceId = instanceId;
    this.instanceUid = instanceUid;
  }

  public int getInstanceId() {
    return instanceId;
  }

  @Nullable
  public String getInstanceUid() {
    return instanceUid;
  }

  @Override
  public String toString() {
    return "PreviewRequestPollerInfo{" +
      "instanceId=" + instanceId +
      ", instanceUid='" + instanceUid + '\'' +
      '}';
  }
}
