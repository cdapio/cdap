/*
 * Copyright © 2021-2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.app.services.AbstractServiceDiscoverer;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.NettyHttpService;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class OAuthAccessTokenMacroEvaluatorTest {

  private static final String PROVIDER = "test";
  private static final String CREDENTIAL_ID = "testcredential";

  private static NettyHttpService httpService;
  private static ServiceDiscoverer serviceDiscoverer;

  @BeforeClass
  public static void init() throws Exception {
    httpService = NettyHttpService.builder("OAuthTest")
      .setHttpHandlers(new MockOauthHandler(
        ImmutableMap.of(
          PROVIDER, ImmutableMap.of(CREDENTIAL_ID, new MockOauthHandler.OAuthInfo("foobar", "bearer"))
        )))
      .build();

    httpService.start();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    String discoveryName = ServiceDiscoverable.getName(NamespaceId.SYSTEM.getNamespace(),
                                                       Constants.PIPELINEID, ProgramType.SERVICE,
                                                       Constants.STUDIO_SERVICE_NAME);
    discoveryService.register(new Discoverable(discoveryName, httpService.getBindAddress()));
    RemoteClientFactory remoteClientFactory = new RemoteClientFactory(
      discoveryService, new DefaultInternalAuthenticator(new AuthenticationTestContext()));
    serviceDiscoverer = new AbstractServiceDiscoverer(NamespaceId.DEFAULT.app("testapp").spark("testspark")) {
      @Override
      protected RemoteClientFactory getRemoteClientFactory() {
        return remoteClientFactory;
      }
    };
  }

  @AfterClass
  public static void finish() throws Exception {
    httpService.stop();
  }

  @Test
  public void testOAuthAccessTokenMacro() {
    MacroEvaluator macroEvaluator = new OAuthAccessTokenMacroEvaluator(serviceDiscoverer);
    String accessToken = macroEvaluator.evaluate(OAuthAccessTokenMacroEvaluator.FUNCTION_NAME,
                                                                PROVIDER, CREDENTIAL_ID);
    // assert contain all properties
    Assert.assertEquals("foobar", accessToken);
  }
}
