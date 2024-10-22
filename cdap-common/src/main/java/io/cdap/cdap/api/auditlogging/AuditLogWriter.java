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

import io.cdap.cdap.security.spi.authorization.AuditLogContext;

import java.io.IOException;
import java.util.Queue;

/**
 * An interface to write/ persist a collection of {@link AuditLogContext} to a
 * messaging service / topic  ( ex - tms )
 *
 */
public interface AuditLogWriter {

  /**
   * pushes the log entry to respective messaging topic
   */
  void publish(Queue<AuditLogContext> auditLogContexts) throws IOException;
}
