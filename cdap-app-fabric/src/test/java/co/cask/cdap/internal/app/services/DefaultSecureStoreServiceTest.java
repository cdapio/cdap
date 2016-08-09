/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsService;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.proto.security.SecureKeyListEntry;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class DefaultSecureStoreServiceTest {
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final String KEY1 = "key1";
  private static final String DESCRIPTION1 = "This is the first key";
  private static final String VALUE1 = "caskisgreat";

  private static SecureStoreService secureStoreService;
  private static Authorizer authorizer;
  private static AuthorizationEnforcementService authorizationEnforcementService;
  private static AppFabricServer appFabricServer;
  private static RemoteSystemOperationsService remoteSystemOperationsService;

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");
    final Injector injector = Guice.createInjector(new AppFabricTestModule(createCConf(), sConf));
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    authorizationEnforcementService = injector.getInstance(AuthorizationEnforcementService.class);
    authorizationEnforcementService.startAndWait();
    remoteSystemOperationsService = injector.getInstance(RemoteSystemOperationsService.class);
    remoteSystemOperationsService.startAndWait();
    secureStoreService = injector.getInstance(SecureStoreService.class);
    authorizer = injector.getInstance(AuthorizerInstantiator.class).get();
    SecurityRequestContext.setUserId(ALICE.getName());

    secureStoreService = injector.getInstance(SecureStoreService.class);
    authorizer.grant(NamespaceId.DEFAULT, ALICE, Collections.singleton(Action.READ));
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return injector.getInstance(NamespaceAdmin.class).exists(Id.Namespace.DEFAULT);
      }
    }, 5, TimeUnit.SECONDS);
    authorizer.revoke(NamespaceId.DEFAULT, ALICE, Collections.singleton(Action.READ));
  }

  private static CConfiguration createCConf() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMPORARY_FOLDER.newFolder());
    String secureStoreLocation = locationFactory.create("securestore").toURI().getPath();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Security.Store.FILE_PATH, secureStoreLocation);
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    cConf.set(Constants.Security.Authorization.SUPERUSERS, "hulk");
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());

    // set secure store provider
    cConf.set(Constants.Security.Store.PROVIDER, "file");
    return cConf;
  }

  @Test
  public void testSecureStoreAccess() throws Exception {
    final SecureKeyId secureKeyId1 = NamespaceId.DEFAULT.secureKey(KEY1);
    SecurityRequestContext.setUserId(ALICE.getName());
    final SecureKeyCreateRequest createRequest = new SecureKeyCreateRequest(DESCRIPTION1, VALUE1,
                                                                            Collections.<String, String>emptyMap());
    try {
      secureStoreService.put(secureKeyId1, createRequest);
      Assert.fail("Alice should not be able to store a key since she does not have WRITE privileges on the namespace");
    } catch (UnauthorizedException expected) {
      // expected
    }


    // Grant ALICE write access to the namespace
    grantAndAssertSuccess(NamespaceId.DEFAULT, ALICE, ImmutableSet.of(Action.WRITE));
    // Write should succeed
    secureStoreService.put(secureKeyId1, createRequest);
    // Listing should return the value just written
    List<SecureKeyListEntry> secureKeyListEntries = secureStoreService.list(NamespaceId.DEFAULT);
    Assert.assertEquals(secureKeyListEntries.size(), 1);
    Assert.assertEquals(secureKeyListEntries.get(0).getName(), KEY1);
    Assert.assertEquals(secureKeyListEntries.get(0).getDescription(), DESCRIPTION1);
    revokeAndAssertSuccess(secureKeyId1, ALICE, ImmutableSet.of(Action.ALL));
    secureKeyListEntries = secureStoreService.list(NamespaceId.DEFAULT);
    Assert.assertEquals(0, secureKeyListEntries.size());

    // Give BOB read access and verify that he can read the stored data
    SecurityRequestContext.setUserId(BOB.getName());
    grantAndAssertSuccess(NamespaceId.DEFAULT, BOB, ImmutableSet.of(Action.READ));
    grantAndAssertSuccess(secureKeyId1, BOB, ImmutableSet.of(Action.READ));
    Assert.assertArrayEquals(secureStoreService.get(secureKeyId1).get(), VALUE1.getBytes());
    secureKeyListEntries = secureStoreService.list(NamespaceId.DEFAULT);
    Assert.assertEquals(1, secureKeyListEntries.size());

    // BOB should not be able to delete the key
    try {
      secureStoreService.delete(secureKeyId1);
      Assert.fail("Bob should not be able to delete a key since he does not have ADMIN privileges on the key");
    } catch (UnauthorizedException expected) {
      // expected
    }

    // Grant Bob ADMIN access and he should be able to delete the key
    grantAndAssertSuccess(secureKeyId1, BOB, ImmutableSet.of(Action.ADMIN));
    secureStoreService.delete(secureKeyId1);
    Assert.assertEquals(0, secureStoreService.list(NamespaceId.DEFAULT).size());
    Predicate<Privilege> secureKeyIdFilter = new Predicate<Privilege>() {
      @Override
      public boolean apply(Privilege input) {
        return input.getEntity().equals(secureKeyId1);
      }
    };
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(ALICE), secureKeyIdFilter).isEmpty());
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(BOB), secureKeyIdFilter).isEmpty());
  }

  @AfterClass
  public static void cleanup() {
    appFabricServer.stopAndWait();
    remoteSystemOperationsService.stopAndWait();
    authorizationEnforcementService.stopAndWait();
  }

  private void grantAndAssertSuccess(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    Set<Privilege> existingPrivileges = authorizer.listPrivileges(principal);
    authorizer.grant(entityId, principal, actions);
    ImmutableSet.Builder<Privilege> expectedPrivilegesAfterGrant = ImmutableSet.builder();
    for (Action action : actions) {
      expectedPrivilegesAfterGrant.add(new Privilege(entityId, action));
    }
    Assert.assertEquals(Sets.union(existingPrivileges, expectedPrivilegesAfterGrant.build()),
                        authorizer.listPrivileges(principal));
  }

  private void revokeAndAssertSuccess(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    Set<Privilege> existingPrivileges = authorizer.listPrivileges(principal);
    authorizer.revoke(entityId, principal, actions);
    for (Action action : actions) {
      existingPrivileges.remove(new Privilege(entityId, action));
    }
    Assert.assertEquals(existingPrivileges, authorizer.listPrivileges(principal));
  }
}
