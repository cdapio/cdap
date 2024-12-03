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

package io.cdap.cdap.common.guice;

import com.google.inject.AbstractModule;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;

/**
 * A NO OPERATION guice module for {@link AuditLogWriter}.
 * This is to be used in modules where AUDIT LOGGING is not expected, but it is a part of common netty layer binding,
 * So it needs to be included.
 */
public class NoOpAuditLogModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(AuditLogWriter.class).toInstance(auditLogContexts -> {});
  }
}
