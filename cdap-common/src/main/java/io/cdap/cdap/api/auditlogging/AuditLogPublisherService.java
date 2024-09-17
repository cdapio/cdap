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

package io.cdap.cdap.api.auditlogging;


import com.google.common.util.concurrent.Service;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;

import java.util.Queue;


/**
 * Service to batch and publish audit log to external auth service.
 */
public interface AuditLogPublisherService extends Service {

  /**
   * pushes the log entry to respective external service
   */
  void publish();

  /**
   * add to service's pending list for publishing
   */
  void addAuditContexts(Queue<AuditLogContext> auditLogContextQueue);
}