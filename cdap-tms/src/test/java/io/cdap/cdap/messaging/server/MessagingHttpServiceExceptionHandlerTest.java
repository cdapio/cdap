/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.messaging.server;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.messaging.DefaultTopicMetadata;
import io.cdap.cdap.messaging.client.DefaultClientMessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import java.net.InetAddress;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Fast unit test for {@link MessagingHttpService} exception handler and user ID logging fallback.
 */
public class MessagingHttpServiceExceptionHandlerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testBadRequestExceptionHandlerWithNullAndNonNullUserId() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.MessagingSystem.HTTP_SERVER_BIND_ADDRESS,
        InetAddress.getLocalHost().getHostName());

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new InMemoryDiscoveryModule(),
        new AuthenticationContextModules().getNoOpModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
          }
        });

    MessagingHttpService httpService = injector.getInstance(MessagingHttpService.class);
    httpService.startAsync().awaitRunning();
    try {
      MessagingService client = new DefaultClientMessagingService(
          injector.getInstance(RemoteClientFactory.class), false);
      TopicId topicId = NamespaceId.DEFAULT.topic("invalidTtlTopic");

      String previousUser = SecurityRequestContext.getUserId();
      try {
        // Case 1: null user ID triggers firstNonNull(..., "<null>") fallback
        SecurityRequestContext.setUserId(null);
        try {
          client.createTopic(new DefaultTopicMetadata(topicId, "ttl", "not-a-number"));
          Assert.fail("Expected IllegalArgumentException for invalid ttl");
        } catch (IllegalArgumentException e) {
          Assert.assertTrue(e.getMessage().contains("Failed to create topic"));
        }

        // Case 2: non-null user ID exercises firstNonNull primary value
        SecurityRequestContext.setUserId("alice");
        try {
          client.createTopic(new DefaultTopicMetadata(topicId, "ttl", "still-not-a-number"));
          Assert.fail("Expected IllegalArgumentException for invalid ttl");
        } catch (IllegalArgumentException e) {
          Assert.assertTrue(e.getMessage().contains("Failed to create topic"));
        }
      } finally {
        SecurityRequestContext.setUserId(previousUser);
      }
    } finally {
      httpService.stopAsync().awaitTerminated();
    }
  }
}
