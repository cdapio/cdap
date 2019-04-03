/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.data2.audit;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.audit.AuditPayload;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.id.EntityId;

/**
 * No-op audit publisher.
 */
public class NoOpAuditPublisher implements AuditPublisher {

  @Override
  public void publish(EntityId entityId, AuditType auditType, AuditPayload auditPayload) {
    // no-op
  }

  @Override
  public void publish(MetadataEntity metadataEntity, AuditType auditType, AuditPayload auditPayload) {
    // no-op
  }
}
