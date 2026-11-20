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

package io.cdap.cdap.gateway.router;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.security.auth.TokenState;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link NettyRouter}.
 */
public class NettyRouterTest {

  @Test
  public void testStartAndStopManagesTokenValidatorWhenManagedSecurityEnabled() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    cConf.setLong(Constants.Router.CCONF_RELOAD_INTERVAL_SECONDS, 0L);

    TrackingTokenValidator tokenValidator = new TrackingTokenValidator();
    NettyRouter router = new NettyRouter(
        cConf,
        SConfiguration.create(),
        InetAddress.getLoopbackAddress(),
        Mockito.mock(RouterServiceLookup.class),
        tokenValidator,
        Mockito.mock(UserIdentityExtractor.class),
        new InMemoryDiscoveryService(),
        new NoOpAeadCipher());

    router.startAndWait();
    try {
      Assert.assertEquals(Service.State.RUNNING, router.state());
      Assert.assertTrue(router.getBoundAddress().isPresent());
      Assert.assertTrue(tokenValidator.started.get());
      Assert.assertTrue(tokenValidator.isRunning());
    } finally {
      router.stopAndWait();
    }

    Assert.assertEquals(Service.State.TERMINATED, router.state());
    Assert.assertFalse(router.getBoundAddress().isPresent());
    Assert.assertTrue(tokenValidator.stopped.get());
    Assert.assertEquals(Service.State.TERMINATED, tokenValidator.state());
  }

  @Test
  public void testStartAndStopSkipsTokenValidatorWhenInternalAuthEnabled() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.setLong(Constants.Router.CCONF_RELOAD_INTERVAL_SECONDS, 0L);

    TrackingTokenValidator tokenValidator = new TrackingTokenValidator();
    NettyRouter router = new NettyRouter(
        cConf,
        SConfiguration.create(),
        InetAddress.getLoopbackAddress(),
        Mockito.mock(RouterServiceLookup.class),
        tokenValidator,
        Mockito.mock(UserIdentityExtractor.class),
        new InMemoryDiscoveryService(),
        new NoOpAeadCipher());

    router.startAndWait();
    router.stopAndWait();

    Assert.assertFalse(tokenValidator.started.get());
    Assert.assertFalse(tokenValidator.stopped.get());
    Assert.assertEquals(Service.State.NEW, tokenValidator.state());
  }

  private static final class TrackingTokenValidator extends AbstractIdleService
      implements TokenValidator {
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    @Override
    protected void startUp() {
      started.set(true);
    }

    @Override
    protected void shutDown() {
      stopped.set(true);
    }

    @Override
    public TokenState validate(String token) {
      return TokenState.VALID;
    }
  }
}
