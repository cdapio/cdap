/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.authorization.RemoteAuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Test {@link RemoteAuthorizationEnforcer} with cache disabled.
 */
public class RemotePrivilegesNoCacheTest extends RemotePrivilegesTestBase {

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    RemotePrivilegesTestBase.setup();
  }

  @Override
  public void testAuthorizationEnforcer() throws Exception {
    super.testAuthorizationEnforcer();

    // The super class revokes all privileges after test is done. Since cache is disabled, all enforce should fail.
    try {
      authorizationEnforcer.enforce(APP, ALICE, Action.ADMIN);
      Assert.fail("Enforce should have failed since cache is disabled");
    } catch (UnauthorizedException e) {
      // Expected
    }

    try {
      authorizationEnforcer.enforce(PROGRAM, ALICE, Action.EXECUTE);
      Assert.fail("Enforce should have failed since cache is disabled");
    } catch (UnauthorizedException e) {
      // Expected
    }
  }

  @Override
  public void testVisibility() throws Exception {
    super.testVisibility();

    // The super class revokes all privileges after test is done. Since cache is disabled, nothing should be visible.
    Assert.assertEquals(ImmutableSet.of(),
                        authorizationEnforcer.isVisible(ImmutableSet.of(NS, APP, PROGRAM), ALICE));
  }
}
