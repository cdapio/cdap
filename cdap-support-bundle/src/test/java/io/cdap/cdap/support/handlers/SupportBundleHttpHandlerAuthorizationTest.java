/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.support.handlers;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.support.services.SupportBundleGenerator;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Tests authorization for {@link SupportBundleHttpHandler} read and download endpoints.
 */
public class SupportBundleHttpHandlerAuthorizationTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private ContextAccessEnforcer enforcer;
  private SupportBundleGenerator generator;
  private HttpResponder responder;
  private SupportBundleHttpHandler handler;
  private String uuid;

  @Before
  public void setUp() throws Exception {
    uuid = UUID.randomUUID().toString();
    enforcer = Mockito.mock(ContextAccessEnforcer.class);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.SupportBundle.SUPPORT_BUNDLE_TEMP_DIR,
        tempFolder.newFolder().getAbsolutePath());
    generator = Mockito.mock(SupportBundleGenerator.class);
    Mockito.when(generator.createBundleZipByRequest(Mockito.eq(uuid), Mockito.any(), Mockito.any()))
        .thenReturn("SHA-256=test");
    responder = Mockito.mock(HttpResponder.class);
    handler = new SupportBundleHttpHandler(cConf, generator, enforcer);
  }

  @Test
  public void testSupportBundleReadsRequireInstanceUpdate() throws Exception {
    Mockito.doThrow(new UnauthorizedException("denied"))
        .when(enforcer).enforce(InstanceId.SELF, StandardPermission.UPDATE);
    assertUnauthorized(() -> handler.listSupportBundles(null, responder));
    assertUnauthorized(() -> handler.getSupportBundleStatus(null, responder, uuid));
    FullHttpRequest emptyPost = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3/support/bundles/" + uuid,
        Unpooled.EMPTY_BUFFER);
    assertUnauthorized(() -> handler.downloadSupportBundle(emptyPost, responder, uuid));
    Mockito.verifyNoInteractions(generator);

    Mockito.reset(enforcer);
    handler.listSupportBundles(null, responder);
    handler.getSupportBundleStatus(null, responder, uuid);
    handler.downloadSupportBundle(emptyPost, responder, uuid);
    Mockito.verify(enforcer, Mockito.times(3))
        .enforce(InstanceId.SELF, StandardPermission.UPDATE);
    Mockito.verify(generator).getAllBundleStatus();
    Mockito.verify(generator).getBundle(uuid);
  }

  private static void assertUnauthorized(ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
