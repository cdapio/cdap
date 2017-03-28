/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.kerberos;

import co.cask.cdap.proto.id.NamespacedEntityId;

import java.util.Objects;

/**
 * A wrapper which wraps around the {@link co.cask.cdap.proto.id.NamespacedEntityId} on which impersonation needs to
 * be performed and the type of operation {@link ImpersonatedOpType} which will be performed.
 */
public class ImpersonationRequest {
  private final NamespacedEntityId entityId;
  private final ImpersonatedOpType impersonatedOpType;

  public ImpersonationRequest(NamespacedEntityId entityId, ImpersonatedOpType impersonatedOpType) {
    this.entityId = entityId;
    this.impersonatedOpType = impersonatedOpType;
  }

  public NamespacedEntityId getEntityId() {
    return entityId;
  }

  public ImpersonatedOpType getImpersonatedOpType() {
    return impersonatedOpType;
  }

  @Override
  public String toString() {
    return "ImpersonationRequest{" +
      "entityId=" + entityId +
      ", impersonatedOpType=" + impersonatedOpType +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ImpersonationRequest that = (ImpersonationRequest) o;
    return Objects.equals(entityId, that.entityId) &&
      impersonatedOpType == that.impersonatedOpType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, impersonatedOpType);
  }
}
