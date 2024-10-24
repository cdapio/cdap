/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Queue;

/**
 * An SPI that delegates the collection of {@link AuditLogContext}s to an extension that will publish the log events
 * to the respective destination.
 */
@Beta
public interface AuditLoggerSpi {
  /**
   * The status of a call for authorization check.
   */
  enum PublishStatus {
    PUBLISHED,
    UNSUCCESSFUL
  }

  /**
   * TODO : THIS IS WIP : Needs to be modified based on how auth extension works.
   * Specially w.r.t to Retry.
   * If the auth ext is able to publish a batch all together vs needs to publish one by one.
   * @return {@link PublishStatus}
   */
  PublishStatus publish(Queue<AuditLogContext> auditLogContexts);

}
