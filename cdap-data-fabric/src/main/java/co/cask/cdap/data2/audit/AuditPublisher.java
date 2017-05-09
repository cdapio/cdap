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

package co.cask.cdap.data2.audit;

import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.EntityId;

/**
 * AuditPublisher is used to publish non-data changes to entities that need to be logged for audit purpose.
 */
public interface AuditPublisher {

  /**
   * Publish audit log for a change.
   *
   * @param entityId entity affected by the change
   * @param auditType type of change
   * @param auditPayload information on the change
   */
  void publish(EntityId entityId, AuditType auditType, AuditPayload auditPayload);
}
