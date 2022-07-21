/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.remote;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.RemoteAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Test {@link RemoteAccessEnforcer} with cache disabled.
 */
public class RemotePermissionsNoCacheTest extends RemotePermissionsTestBase {

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    RemotePermissionsTestBase.setup();
  }

  @Override
  public void testAccessEnforcer() throws Exception {
    super.testAccessEnforcer();

    // The super class revokes all privileges after test is done. Since cache is disabled, all enforce should fail.
    try {
      accessEnforcer.enforce(APP, ALICE, StandardPermission.UPDATE);
      Assert.fail("Enforce should have failed since cache is disabled");
    } catch (UnauthorizedException e) {
      // Expected
    }

    try {
      accessEnforcer.enforce(PROGRAM, ALICE, ApplicationPermission.EXECUTE);
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
                        accessEnforcer.isVisible(ImmutableSet.of(NS, APP, PROGRAM), ALICE));
  }
}
