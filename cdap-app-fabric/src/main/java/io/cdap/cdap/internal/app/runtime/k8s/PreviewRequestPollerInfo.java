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

import com.google.common.base.Objects;
import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * A holder for information that uniquely identifies and authenticates a preview runner. This object
 * is serialized and stored in the PreviewStore.
 */
public class PreviewRequestPollerInfo {

  private final int instanceId;
  private final String instanceUid;
  private final byte[] secureToken;

  /**
   * Constructs a new PreviewRequestPollerInfo.
   *
   * @param instanceId  the instance id of the runner
   * @param instanceUid the unique UID of the runner pod
   * @param secureToken a secret token used to authenticate requests from the runner
   */
  public PreviewRequestPollerInfo(int instanceId, @Nullable String instanceUid,
      @Nullable byte[] secureToken) {
    this.instanceId = instanceId;
    this.instanceUid = instanceUid;
    this.secureToken = secureToken;
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
    return "PreviewRequestPollerInfo{" + "instanceId=" + instanceId + ", instanceUid='"
        + instanceUid + '\'' + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(instanceId, instanceUid, Arrays.hashCode(secureToken));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PreviewRequestPollerInfo that = (PreviewRequestPollerInfo) o;
    return instanceId == that.instanceId && Objects.equal(instanceUid, that.instanceUid)
        && Arrays.equals(secureToken, that.secureToken);
  }
}
