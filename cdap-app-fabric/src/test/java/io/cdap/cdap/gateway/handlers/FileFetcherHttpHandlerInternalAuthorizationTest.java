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

package io.cdap.cdap.gateway.handlers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * FileFetcherHttpHandlerInternal#download() resolves the requested path as an absolute
 * location against the whole storage backend (not scoped to any namespace or
 * caller-owned resource), so it must reject callers without instance-level access,
 * same as ConfigHandler does for its own raw-internal-state endpoints. See
 * ConfigHandlerAuthorizationTest for the sibling coverage this mirrors.
 */
public class FileFetcherHttpHandlerInternalAuthorizationTest {
  private static final Principal MASTER_PRINCIPAL = new Principal("master", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL = new Principal("unprivileged",
                                                                        Principal.PrincipalType.USER);

  private static FileFetcherHttpHandlerInternal fileFetcherHandler;

  HttpRequest request;
  HttpResponder responder;
  Exception exceptionThrown;

  @BeforeClass
  public static void setup() {
    StandardPermission[] requiredPermissions = new StandardPermission[] {StandardPermission.GET};

    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    inMemoryAccessController.grant(Authorizable.fromEntityId(InstanceId.SELF), MASTER_PRINCIPAL,
                                   Collections.unmodifiableSet(new HashSet<>(Arrays.asList(requiredPermissions))));
    AuthenticationContext authenticationContext = new AuthenticationTestContext();
    DefaultContextAccessEnforcer contextAccessEnforcer = new DefaultContextAccessEnforcer(authenticationContext,
                                                                                          inMemoryAccessController);

    LocationFactory locationFactory = new LocalLocationFactory(new File(System.getProperty("java.io.tmpdir")));
    fileFetcherHandler = new FileFetcherHttpHandlerInternal(locationFactory, contextAccessEnforcer);
  }

  @Before
  public void initializeVariables() {
    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
    exceptionThrown = null;
  }

  @Test
  public void testDownloadUnauthorized() throws Exception {
    when(request.uri()).thenReturn("/v3Internal/location/does-not-matter");
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      fileFetcherHandler.download(request, responder);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    }
    Assert.assertNotNull("an unprivileged caller must not be able to fetch an arbitrary "
        + "storage-backend path via the internal file-fetch endpoint", exceptionThrown);
  }

  @Test
  public void testDownloadAuthorizedReachesLocationLookup() throws Exception {
    when(request.uri()).thenReturn("/v3Internal/location/does-not-exist");
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    try {
      fileFetcherHandler.download(request, responder);
    } catch (UnauthorizedException e) {
      exceptionThrown = e;
    } catch (NotFoundException e) {
      // Expected: a caller with instance-level access passes the authorization check
      // and proceeds to the real location lookup, which correctly reports the
      // requested (nonexistent) path as not found. This is not an authorization
      // failure.
    }
    Assert.assertNull("a caller with instance-level access must not be rejected by "
        + "the authorization check", exceptionThrown);
  }
}
