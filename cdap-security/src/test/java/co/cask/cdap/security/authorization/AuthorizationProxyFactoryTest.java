/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.EntityType;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import co.cask.cdap.api.security.PrincipalType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RequestContext;
import co.cask.cdap.security.exception.UnauthorizedException;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base test for {@link AuthorizationProxyFactory} implementations.
 */
public abstract class AuthorizationProxyFactoryTest {

  protected abstract Class<? extends AuthorizationProxyFactory> getProxyFactoryClass();

  @Test
  public void testAuthorized() throws Exception {
    Injector injector = createInjector();
    ACLClient aclClient = injector.getInstance(ACLClient.class);
    AuthorizationProxyFactory proxyFactory = injector.getInstance(AuthorizationProxyFactory.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");
    aclClient.setAclForUser(secretEntity, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.ADMIN));

    RequestContext.setEntityId(secretEntity);
    RequestContext.setUserId(bobUser.getId());

    TestRequiresPermissions testRequiresPermissions = proxyFactory.wrap(new TestRequiresPermissions());
    testRequiresPermissions.doWithAdmin();
  }

  @Test
  public void testNoContext() throws Exception {
    Injector injector = createInjector();
    ACLClient aclClient = injector.getInstance(ACLClient.class);
    AuthorizationProxyFactory proxyFactory = injector.getInstance(AuthorizationProxyFactory.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");
    aclClient.setAclForUser(secretEntity, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.ADMIN));

    TestRequiresPermissions testRequiresPermissions = proxyFactory.wrap(new TestRequiresPermissions());
    try {
      testRequiresPermissions.doWithAdmin();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // GOOD
    }
  }

  @Test
  public void testNoPermission() throws Exception {
    Injector injector = createInjector();
    ACLClient aclClient = injector.getInstance(ACLClient.class);
    AuthorizationProxyFactory proxyFactory = injector.getInstance(AuthorizationProxyFactory.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");
    aclClient.setAclForUser(secretEntity, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.WRITE));

    RequestContext.setEntityId(secretEntity);
    RequestContext.setUserId(bobUser.getId());

    TestRequiresPermissions testRequiresPermissions = proxyFactory.wrap(new TestRequiresPermissions());
    try {
      testRequiresPermissions.doWithAdmin();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // GOOD
    }
  }

  @Test
  public void testNoACL() throws Exception {
    Injector injector = createInjector();
    ACLClient aclClient = injector.getInstance(ACLClient.class);
    AuthorizationProxyFactory proxyFactory = injector.getInstance(AuthorizationProxyFactory.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");

    RequestContext.setEntityId(secretEntity);
    RequestContext.setUserId(bobUser.getId());

    TestRequiresPermissions testRequiresPermissions = proxyFactory.wrap(new TestRequiresPermissions());
    try {
      testRequiresPermissions.doWithAdmin();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // GOOD
    }
  }

  private Injector createInjector() {
    return Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ACLClient.class).toInstance(new InMemoryACLClient());
          CConfiguration cConf = CConfiguration.create();
          cConf.setBoolean(Constants.Security.CFG_SECURITY_ENABLED, true);
          bind(CConfiguration.class).toInstance(cConf);
          bind(AuthorizationProxyFactory.class).to(getProxyFactoryClass());
        }
      });
  }

  /**
   * Contains a RequiresPermissions annotated method for testing.
   */
  public static class TestRequiresPermissions {
    @RequiresPermissions({ PermissionType.ADMIN })
    public void doWithAdmin() { }
  }

}
