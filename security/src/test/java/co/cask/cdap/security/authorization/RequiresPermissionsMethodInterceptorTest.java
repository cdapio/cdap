/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.common.http.SecurityRequestContext;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;
import com.google.inject.util.Providers;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.NotAuthorizedException;

/**
 * Test for {@link RequiresPermissionsMethodInterceptor}.
 */
public class RequiresPermissionsMethodInterceptorTest {

  @Test
  public void testAuthorized() throws Exception {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        InMemoryACLClient aclClient = new InMemoryACLClient();
        bind(ACLClient.class).toInstance(aclClient);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(RequiresPermissions.class),
                        new RequiresPermissionsMethodInterceptor(Providers.of(aclClient), getProvider(CConfiguration.class)));
      }
    });

    ACLClient aclClient = injector.getInstance(ACLClient.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");
    aclClient.setACLForUser(secretEntity, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.ADMIN));

    SecurityRequestContext.setEntityId(secretEntity);
    SecurityRequestContext.setUserId(bobUser.getId());

    TestRequiresPermissions testRequiresPermissions = injector.getInstance(TestRequiresPermissions.class);
    testRequiresPermissions.doWithAdmin();
  }

  @Test
  public void testNoContext() throws Exception {
    Injector injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          InMemoryACLClient aclClient = new InMemoryACLClient();
          bind(ACLClient.class).toInstance(aclClient);
          bindInterceptor(Matchers.any(), Matchers.annotatedWith(RequiresPermissions.class),
                          new RequiresPermissionsMethodInterceptor(Providers.of(aclClient), getProvider(CConfiguration.class)));
        }
      }
    );

    ACLClient aclClient = injector.getInstance(ACLClient.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");
    aclClient.setACLForUser(secretEntity, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.ADMIN));

    TestRequiresPermissions testRequiresPermissions = injector.getInstance(TestRequiresPermissions.class);
    try {
      testRequiresPermissions.doWithAdmin();
      Assert.fail("Expected NotAuthorizedException");
    } catch (NotAuthorizedException e) {
      // GOOD
    }
  }

  @Test
  public void testNoPermission() throws Exception {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        InMemoryACLClient aclClient = new InMemoryACLClient();
        bind(ACLClient.class).toInstance(aclClient);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(RequiresPermissions.class),
                        new RequiresPermissionsMethodInterceptor(Providers.of(aclClient), getProvider(CConfiguration.class)));
      }
    });

    ACLClient aclClient = injector.getInstance(ACLClient.class);

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");
    aclClient.setACLForUser(secretEntity, bobUser.getId(), ImmutableList.of(PermissionType.READ, PermissionType.WRITE));

    SecurityRequestContext.setEntityId(secretEntity);
    SecurityRequestContext.setUserId(bobUser.getId());

    TestRequiresPermissions testRequiresPermissions = injector.getInstance(TestRequiresPermissions.class);
    try {
      testRequiresPermissions.doWithAdmin();
      Assert.fail("Expected NotAuthorizedException");
    } catch (NotAuthorizedException e) {
      // GOOD
    }
  }

  @Test
  public void testNoACL() throws Exception {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        InMemoryACLClient aclClient = new InMemoryACLClient();
        bind(ACLClient.class).toInstance(aclClient);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(RequiresPermissions.class),
                        new RequiresPermissionsMethodInterceptor(Providers.of(aclClient), getProvider(CConfiguration.class)));
      }
    });

    final Principal bobUser = new Principal(PrincipalType.USER, "bob");
    final EntityId secretEntity = new EntityId(EntityType.STREAM, "secretEntity");

    SecurityRequestContext.setEntityId(secretEntity);
    SecurityRequestContext.setUserId(bobUser.getId());

    TestRequiresPermissions testRequiresPermissions = injector.getInstance(TestRequiresPermissions.class);
    try {
      testRequiresPermissions.doWithAdmin();
      Assert.fail("Expected NotAuthorizedException");
    } catch (NotAuthorizedException e) {
      // GOOD
    }
  }

  /**
   * Contains a RequiresPermissions annotated method for testing.
   */
  public static class TestRequiresPermissions {
    @RequiresPermissions({ PermissionType.ADMIN })
    public void doWithAdmin() { }
  }

}
