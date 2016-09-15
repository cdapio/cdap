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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Tests for {@link DatasetService} with authorization enabled
 */
public class DatasetServiceAuthorizationTest extends DatasetServiceTestBase {
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);

  private static Authorizer authorizer;

  @BeforeClass
  public static void setup() throws Exception {
    locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    initializeAndStartService(createCConf());
    authorizer = injector.getInstance(AuthorizerInstantiator.class).get();
  }

  protected static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = DatasetServiceTestBase.createCConf();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    return cConf;
  }

  @Test
  public void testDatasetInstances() throws Exception {
    final DatasetId dsId = NamespaceId.DEFAULT.dataset("myds");
    final DatasetId dsId1 = NamespaceId.DEFAULT.dataset("myds1");
    DatasetId dsId2 = NamespaceId.DEFAULT.dataset("myds2");
    SecurityRequestContext.setUserId(ALICE.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY);
      }
    }, "Alice should not be able to add a dataset instance since she does not have WRITE privileges on the namespace");
    // grant alice write access to the namespace
    grantAndAssertSuccess(NamespaceId.DEFAULT, ALICE, ImmutableSet.of(Action.WRITE));
    // now adding an instance should succeed
    addInstanceAndAssertPrivileges(ALICE, dsId);
    // alice should be able to perform all operations on the dataset
    Assert.assertTrue(dsFramework.hasInstance(dsId));
    Assert.assertNotNull(dsFramework.getDataset(dsId, ImmutableMap.<String, String>of(), null));
    dsFramework.updateInstance(dsId, DatasetProperties.builder().add("key", "value").build());
    // operations should fail for bob
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.getDataset(dsId, ImmutableMap.<String, String>of(), null);
      }
    }, String.format("Expected %s to not be have access to %s.", BOB, dsId));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.updateInstance(dsId, DatasetProperties.builder().add("key", "val").build());
      }
    }, String.format("Expected %s to not be have %s privilege on %s.", BOB, Action.ADMIN, dsId));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.truncateInstance(dsId);
      }
    }, String.format("Expected %s to not be have %s privilege on %s.", BOB, Action.ADMIN, dsId));
    grantAndAssertSuccess(dsId, BOB, ImmutableSet.of(Action.ADMIN));
    // now update should succeed
    dsFramework.updateInstance(dsId, DatasetProperties.builder().add("key", "val").build());
    // as should truncate
    dsFramework.truncateInstance(dsId);
    DatasetSpecification datasetSpec = dsFramework.getDatasetSpec(dsId);
    Assert.assertNotNull(datasetSpec);
    Assert.assertEquals("val", datasetSpec.getProperty("key"));
    // grant Bob WRITE on namespace, so he can add some datasets
    grantAndAssertSuccess(NamespaceId.DEFAULT, BOB, ImmutableSet.of(Action.WRITE));
    addInstanceAndAssertPrivileges(BOB, dsId1);
    addInstanceAndAssertPrivileges(BOB, dsId2);
    // since Bob now has some privileges on all datasets, the list API should return all datasets for him
    Assert.assertEquals(ImmutableSet.of(dsId, dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    // Since Alice has some privileges on the namespace, the list API should return all datasets under the namespace
    SecurityRequestContext.setUserId(ALICE.getName());
    Assert.assertEquals(ImmutableSet.of(dsId, dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // Alice should only be able to delete datasets that she is the ADMIN
    dsFramework.deleteAllInstances(NamespaceId.DEFAULT);
    // alice should now see instances that she can't delete, but have some privileges.
    Assert.assertEquals(ImmutableSet.of(dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // should get an authorization error if alice tries to delete datasets that she does not have permissions on
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteInstance(dsId1);
      }
    }, String.format("Alice should not be able to delete instance %s since she does not have privileges", dsId1));
    grantAndAssertSuccess(dsId1, ALICE, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(ImmutableSet.of(dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    // since Alice now is ADMIN for dsId1, she should be able to delete it
    deleteAndAssertPrivileges(dsId1);

    // Now Alice only see dsId2 from list.
    Assert.assertEquals(ImmutableSet.of(dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // Bob should still see 1 instance
    SecurityRequestContext.setUserId(BOB.getName());
    Assert.assertEquals(ImmutableSet.of(dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    deleteAndAssertPrivileges(dsId2);
  }

  @Test
  public void testNotFound() throws Exception {
    String namespace = NamespaceId.DEFAULT.getNamespace();
    final DatasetId nonExistingInstance = NamespaceId.DEFAULT.dataset("notfound");
    final DatasetModuleId nonExistingModule = NamespaceId.DEFAULT.datasetModule("notfound");
    final DatasetTypeId nonExistingType = NamespaceId.DEFAULT.datasetType("notfound");
    Assert.assertNull(dsFramework.getDatasetSpec(nonExistingInstance));
    Assert.assertFalse(dsFramework.hasInstance(nonExistingInstance));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.updateInstance(nonExistingInstance, DatasetProperties.EMPTY);
      }
    }, String.format("Expected %s to not exist", nonExistingInstance));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteInstance(nonExistingInstance);
      }
    }, String.format("Expected %s to not exist", nonExistingInstance));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.truncateInstance(nonExistingInstance);
      }
    }, String.format("Expected %s to not exist", nonExistingInstance));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteModule(nonExistingModule);
      }
    }, String.format("Expected %s to not exist", nonExistingModule));
    Assert.assertNull(String.format("Expected %s to not exist", nonExistingType),
                      dsFramework.getTypeInfo(nonExistingType));
  }

  @Test
  public void testDatasetTypes() throws Exception {
    final DatasetModuleId module1 = NamespaceId.DEFAULT.datasetModule("module1");
    final DatasetModuleId module2 = NamespaceId.DEFAULT.datasetModule("module2");
    final DatasetTypeId type1 = NamespaceId.DEFAULT.datasetType("datasetType1");
    DatasetTypeId type1x = NamespaceId.DEFAULT.datasetType("datasetType1x");
    final DatasetTypeId type2 = NamespaceId.DEFAULT.datasetType("datasetType2");
    SecurityRequestContext.setUserId(ALICE.getName());
    final Location moduleJar = createModuleJar(TestModule1x.class);
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addModule(module1, new TestModule1x(), moduleJar);
      }
    }, String.format("Expected module add operation to fail for %s because she does not have %s on %s",
                     ALICE, Action.WRITE, NamespaceId.DEFAULT));
    // grant alice WRITE on the namespace
    grantAndAssertSuccess(NamespaceId.DEFAULT, ALICE, EnumSet.of(Action.WRITE));
    dsFramework.addModule(module1, new TestModule1x(), moduleJar);
    // all operations on module1 should succeed as alice
    Assert.assertNotNull(dsFramework.getTypeInfo(type1));
    Assert.assertNotNull(dsFramework.getTypeInfo(type1x));
    // should be able to use the type from the module to add an instance as well
    dsFramework.addInstance(type1x.getType(), NamespaceId.DEFAULT.dataset("succeed"), DatasetProperties.EMPTY);
    // but should fail as Bob
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(type1.getType(), NamespaceId.DEFAULT.dataset("fail"), DatasetProperties.EMPTY);
      }
    }, String.format(
      "Creating an instance of a type from %s should fail as %s does not have any privileges on it.", module1, BOB));

    // granting WRITE on the namespace, BOB should now be able to create the dataset type
    grantAndAssertSuccess(NamespaceId.DEFAULT, BOB, EnumSet.of(Action.WRITE));

    // adding a module should now succeed as bob though, because bob has write privileges on the namespace
    dsFramework.addModule(module2, new TestModule2(), createModuleJar(TestModule2.class));
    // all operations on module2 should succeed as Bob
    Assert.assertNotNull(dsFramework.getTypeInfo(type2));

    // but should fail as Alice after revoking Alice rights to the namespace
    SecurityRequestContext.setUserId(ALICE.getName());
    authorizer.revoke(NamespaceId.DEFAULT, ALICE, EnumSet.allOf(Action.class));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(type2.getType(), NamespaceId.DEFAULT.dataset("fail"), DatasetProperties.EMPTY);
      }
    }, String.format(
      "Creating an instance of a type from %s should fail as %s does not have any privileges on it.", module2, ALICE));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteModule(module2);
      }
    }, String.format("Deleting module %s should fail as %s does not have any privileges on it.", module2, ALICE));
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteModule(module1);
      }
    }, String.format("Deleting module %s should fail as %s does not have any privileges on it.", module1, BOB));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteAllModules(NamespaceId.DEFAULT);
      }
    }, String.format("Deleting all modules in %s should fail as %s does not have ADMIN privileges on it.",
                     NamespaceId.DEFAULT, BOB));

    // Grant all permission for cleanup
    grantAndAssertSuccess(NamespaceId.DEFAULT, ALICE, EnumSet.allOf(Action.class));
    grantAndAssertSuccess(NamespaceId.DEFAULT, BOB, EnumSet.allOf(Action.class));

    // delete all instances so modules can be deleted
    dsFramework.deleteAllInstances(NamespaceId.DEFAULT);
    SecurityRequestContext.setUserId(ALICE.getName());
    dsFramework.deleteAllInstances(NamespaceId.DEFAULT);
    SecurityRequestContext.setUserId(BOB.getName());
    // After granting admin on the default namespace, deleting all modules should succeed
    grantAndAssertSuccess(NamespaceId.DEFAULT, BOB, EnumSet.of(Action.ADMIN));
    dsFramework.deleteAllModules(NamespaceId.DEFAULT);
  }

  @After
  public void cleanup() throws Exception {
    authorizer.revoke(NamespaceId.DEFAULT);
  }

  private Set<DatasetId> summaryToDatasetIdSet(Collection<DatasetSpecificationSummary> datasetSpecs) {
    Collection<DatasetId> datasetIds =
      Collections2.transform(datasetSpecs, new Function<DatasetSpecificationSummary, DatasetId>() {
        @Override
        public DatasetId apply(DatasetSpecificationSummary input) {
          return NamespaceId.DEFAULT.dataset(input.getName());
        }
      });
    return ImmutableSet.copyOf(datasetIds);
  }

  private void addInstanceAndAssertPrivileges(Principal principal, DatasetId dsId) throws Exception {
    Set<Privilege> before = authorizer.listPrivileges(principal);
    dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY);
    Set<Privilege> after = authorizer.listPrivileges(principal);
    Assert.assertTrue(after.containsAll(before));
    Assert.assertEquals(ImmutableSet.of(new Privilege(dsId, Action.ADMIN), new Privilege(dsId, Action.READ),
                                        new Privilege(dsId, Action.WRITE), new Privilege(dsId, Action.EXECUTE)),
                        Sets.difference(after, before).immutableCopy());
  }

  private void deleteAndAssertPrivileges(final DatasetId dsId) throws Exception {
    dsFramework.deleteInstance(dsId);
    Predicate<Privilege> dsIdFilter = new Predicate<Privilege>() {
      @Override
      public boolean apply(Privilege input) {
        return input.getEntity().equals(dsId);
      }
    };
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(ALICE), dsIdFilter).isEmpty());
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(BOB), dsIdFilter).isEmpty());
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

  private void assertNotFound(DatasetOperationExecutor operation, String failureMsg) throws Exception {
    try {
      operation.execute();
      Assert.fail(failureMsg);
    } catch (InstanceNotFoundException expected) {
      // expected
    } catch (DatasetManagementException e) {
      // no other way to detect errors from DatasetServiceClient
      Assert.assertTrue(e.getMessage().contains("Response code: 404, message: 'Not Found'"));
    }
  }

  private void assertAuthorizationFailure(DatasetOperationExecutor operation, String failureMsg) throws Exception {
    try {
      operation.execute();
      Assert.fail(failureMsg);
    } catch (UnauthorizedException expected) {
      // expected
    } catch (DatasetManagementException e) {
      // no other way to detect errors from DatasetServiceClient
      Assert.assertTrue(e.getMessage().contains("Response code: 403, message: 'Forbidden'"));
    }
  }

  private interface DatasetOperationExecutor {
    void execute() throws Exception;
  }
}
