/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.security.spi.authentication.AccessTokenIdentityValidator;
import javax.annotation.Nullable;

/**
 * Factory to create {@link CommonNettyHttpServiceBuilder} using {@link CConfiguration} and {@link
 * MetricsCollectionService}.
 */
public class CommonNettyHttpServiceFactory {

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final AuditLogWriter auditLogWriter;
  private final AeadCipher userEncryptionAeadCipher;
  private AccessTokenIdentityValidator accessTokenIdentityValidator;

  @Inject
  public CommonNettyHttpServiceFactory(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      AuditLogWriter auditLogWriter,
      @Named(UserCredentialAeadEncryptionModule.USER_CREDENTIAL_ENCRYPTION)
          AeadCipher userEncryptionAeadCipher) {
    this(cConf, metricsCollectionService, auditLogWriter, userEncryptionAeadCipher, null);
  }

  /**
   * Creates a {@link CommonNettyHttpServiceFactory} with an explicit token identity validator.
   *
   * @param cConf CDAP configuration
   * @param metricsCollectionService metrics collection service
   * @param auditLogWriter writer used to publish audit log events
   * @param userEncryptionAeadCipher cipher used for user credential decryption
   * @param accessTokenIdentityValidator validator for CDAP access tokens, if available
   */
  public CommonNettyHttpServiceFactory(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      AuditLogWriter auditLogWriter,
      AeadCipher userEncryptionAeadCipher,
      @Nullable AccessTokenIdentityValidator accessTokenIdentityValidator) {
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.auditLogWriter = auditLogWriter;
    this.userEncryptionAeadCipher = userEncryptionAeadCipher;
    this.accessTokenIdentityValidator = accessTokenIdentityValidator;
  }

  @Inject(optional = true)
  void setAccessTokenIdentityValidator(AccessTokenIdentityValidator accessTokenIdentityValidator) {
    this.accessTokenIdentityValidator = accessTokenIdentityValidator;
  }

  /**
   * Creates a {@link CommonNettyHttpServiceBuilder} with serviceName.
   *
   * @param serviceName Name of the service passed to {@link CommonNettyHttpServiceBuilder}
   * @return {@link CommonNettyHttpServiceBuilder}
   */
  public CommonNettyHttpServiceBuilder builder(String serviceName) {
    return builder(serviceName, true);
  }

  /**
   * Creates a {@link CommonNettyHttpServiceBuilder} with serviceName and worker decryption setting.
   *
   * @param serviceName Name of the service passed to {@link CommonNettyHttpServiceBuilder}
   * @param workerDecryptionEnabled whether worker credential decryption and token verification are
   *     enabled on this service
   * @return {@link CommonNettyHttpServiceBuilder}
   */
  public CommonNettyHttpServiceBuilder builder(String serviceName,
      boolean workerDecryptionEnabled) {
    return new CommonNettyHttpServiceBuilder(cConf, serviceName, metricsCollectionService,
        workerDecryptionEnabled, auditLogWriter, userEncryptionAeadCipher,
        accessTokenIdentityValidator);
  }
}
