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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.support.services.SupportBundleGenerator;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests that support bundle reads need instance-level read access, since bundles hold system logs
 * and pipeline logs from every namespace.
 */
public class SupportBundleHttpHandlerAuthorizationTest {

  private static final String BUNDLE_ID = "bundle";

  @Test
  public void testReadsNeedInstanceLevelRead() throws Exception {
    // Passes every existing bundle check, but has no instance-level read access.
    ContextAccessEnforcer enforcer = mock(ContextAccessEnforcer.class);
    doThrow(new UnauthorizedException("no instance-level read")).when(enforcer)
        .enforceOnParent(EntityType.NAMESPACE, InstanceId.SELF, StandardPermission.LIST);
    SupportBundleHttpHandler handler = new SupportBundleHttpHandler(CConfiguration.create(),
        mock(SupportBundleGenerator.class), enforcer);

    try {
      handler.listSupportBundles(mock(HttpRequest.class), mock(HttpResponder.class));
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    try {
      handler.getSupportBundleStatus(mock(HttpRequest.class), mock(HttpResponder.class), BUNDLE_ID);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  @Test
  public void testReadsAllowedForInstanceLevelReader() throws Exception {
    SupportBundleHttpHandler handler = new SupportBundleHttpHandler(CConfiguration.create(),
        mock(SupportBundleGenerator.class), mock(ContextAccessEnforcer.class));
    handler.listSupportBundles(mock(HttpRequest.class), mock(HttpResponder.class));
    handler.getSupportBundleStatus(mock(HttpRequest.class), mock(HttpResponder.class), BUNDLE_ID);
  }
}
