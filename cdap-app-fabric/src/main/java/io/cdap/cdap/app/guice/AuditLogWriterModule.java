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
 *
 */

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.security.auth.MessagingAuditLogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogWriterModule extends RuntimeModule {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogWriterModule.class);

  private final boolean auditLoggingEnabled;
  private final boolean securityEnabled;

  @Inject
  public AuditLogWriterModule(CConfiguration cConf) {
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    this.auditLoggingEnabled = Feature.DATAPLANE_AUDIT_LOGGING.isEnabled(featureFlagsProvider) ;

    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
  }

  /**
   * Guice modules for In Memory use case of AuditLogWriter.
   * Returns No Op for now. This may change in the future.
   */
  @Override
  public Module getInMemoryModules() {
    return new NoOpAuditLogModule();
  }

  /**
   * Guice modules for Standalone use case of AuditLogWriter.
   * Returns No Op for now. This may change in the future.
   */
  @Override
  public Module getStandaloneModules() {
    return new NoOpAuditLogModule();
  }

  /**
   * Guice modules for Distributed use case where the audit events would be written to a messaging queue like tms.
   */
  @Override
  public Module getDistributedModules() {

    if (auditLoggingEnabled && securityEnabled) {
      LOG.info("Audit Logging feature is ENABLED. Injecting an audit message writer");
      return new AbstractModule() {
        @Override
        protected void configure() {
          bind(AuditLogWriter.class).to(MessagingAuditLogWriter.class).in(Scopes.SINGLETON);
        }
      };
    }

    LOG.debug("Audit Logging feature or Instance security is DISABLED.");
    return new NoOpAuditLogModule();
  }
}
