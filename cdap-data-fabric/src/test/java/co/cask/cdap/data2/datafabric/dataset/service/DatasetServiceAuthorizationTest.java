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
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.security.UserGroupInformation;
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
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    // this is needed since now DefaultAuthorizationEnforcer expects this non-null
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, UserGroupInformation.getLoginUser().getShortUserName());
    return cConf;
  }

  @Test
  public void testDatasetInstances() throws Exception {
    final DatasetId dsId = NamespaceId.DEFAULT.dataset("myds");
    final DatasetId dsId1 = NamespaceId.DEFAULT.dataset("myds1");
    DatasetTypeId tableTypeId = NamespaceId.DEFAULT.datasetType(Table.class.getName());
    DatasetId dsId2 = NamespaceId.DEFAULT.dataset("myds2");
    SecurityRequestContext.setUserId(ALICE.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY);
      }
    }, "Alice should not be able to add a dataset instance since she does not have ADMIN privileges on the dataset");
    // grant alice ADMIN access to the dsId and ADMIN access on the dataset type
    grantAndAssertSuccess(dsId, ALICE, ImmutableSet.of(Action.ADMIN));
    grantAndAssertSuccess(tableTypeId, ALICE, EnumSet.of(Action.ADMIN));
    // now adding an instance should succeed
    dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY);
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
    // grant Bob corresponding privilege to create the dataset
    grantAndAssertSuccess(dsId1, BOB, ImmutableSet.of(Action.ADMIN));
    grantAndAssertSuccess(dsId2, BOB, ImmutableSet.of(Action.ADMIN));
    grantAndAssertSuccess(tableTypeId, BOB, EnumSet.of(Action.ADMIN));
    dsFramework.addInstance(Table.class.getName(), dsId1, DatasetProperties.EMPTY);
    dsFramework.addInstance(Table.class.getName(), dsId2, DatasetProperties.EMPTY);
    // since Bob now has some privileges on all datasets, the list API should return all datasets for him
    Assert.assertEquals(ImmutableSet.of(dsId, dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    // Alice should only be able to see dsId, since she only has privilege on this dataset
    SecurityRequestContext.setUserId(ALICE.getName());
    Assert.assertEquals(ImmutableSet.of(dsId),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // Grant privileges on other datasets to user Alice
    grantAndAssertSuccess(dsId1, ALICE, ImmutableSet.of(Action.EXECUTE));
    grantAndAssertSuccess(dsId2, ALICE, ImmutableSet.of(Action.EXECUTE));

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
    dsFramework.deleteInstance(dsId1);

    // Now Alice only see dsId2 from list.
    Assert.assertEquals(ImmutableSet.of(dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // Bob should still see 1 instance
    SecurityRequestContext.setUserId(BOB.getName());
    Assert.assertEquals(ImmutableSet.of(dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    dsFramework.deleteInstance(dsId2);
  }

  @Test
  public void testNotFound() throws Exception {
    final DatasetId nonExistingInstance = NamespaceId.DEFAULT.dataset("notfound");
    final DatasetModuleId nonExistingModule = NamespaceId.DEFAULT.datasetModule("notfound");
    final DatasetTypeId nonExistingType = NamespaceId.DEFAULT.datasetType("notfound");
    try {
      // user will not be able to get the info about the instance since he does not have any privilege on the instance
      dsFramework.getDatasetSpec(nonExistingInstance);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("is not authorized to perform any one of the actions"));
    }
    try {
      // user will not be able to check the existence on the instance since he does not have any privilege on the
      // instance
      dsFramework.hasInstance(nonExistingInstance);
      Assert.fail();
    } catch (Exception e) {
      // expected
      Assert.assertTrue(e.getMessage().contains("is not authorized to perform any one of the actions"));
    }
    SecurityRequestContext.setUserId(ALICE.getName());
    // user need to have access to the dataset to do any operations, even though the dataset does not exist
    grantAndAssertSuccess(nonExistingInstance, ALICE, EnumSet.of(Action.ADMIN));
    grantAndAssertSuccess(nonExistingModule, ALICE, EnumSet.of(Action.ADMIN));
    // after grant user should be able to check the dataset info
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
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(nonExistingType.getType(), nonExistingInstance, DatasetProperties.EMPTY);
      }
    }, "Alice needs to have READ/ADMIN on the dataset type to create the dataset");
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteModule(nonExistingModule);
      }
    }, String.format("Expected %s to not exist", nonExistingModule));
    grantAndAssertSuccess(nonExistingType, ALICE, EnumSet.of(Action.ADMIN));
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
    DatasetId datasetId = NamespaceId.DEFAULT.dataset("succeed");
    SecurityRequestContext.setUserId(ALICE.getName());
    final Location moduleJar = createModuleJar(TestModule1x.class);
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addModule(module1, new TestModule1x(), moduleJar);
      }
    }, String.format("Expected module add operation to fail for %s because she does not have %s on %s",
                     ALICE, Action.ADMIN, module1));
    // grant alice ADMIN on module1
    grantAndAssertSuccess(module1, ALICE, EnumSet.of(Action.ADMIN));
    // grant all privileges needed to create a dataset
    grantAndAssertSuccess(type1, ALICE, EnumSet.of(Action.ADMIN));
    grantAndAssertSuccess(type1x, ALICE, EnumSet.of(Action.ADMIN));
    grantAndAssertSuccess(datasetId, ALICE, EnumSet.of(Action.ADMIN));
    dsFramework.addModule(module1, new TestModule1x(), moduleJar);
    // all operations on module1 should succeed as alice
    Assert.assertNotNull(dsFramework.getTypeInfo(type1));
    Assert.assertNotNull(dsFramework.getTypeInfo(type1x));
    // should be able to use the type from the module to add an instance as well
    dsFramework.addInstance(type1x.getType(), datasetId, DatasetProperties.EMPTY);
    // but should fail as Bob
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(type1.getType(), NamespaceId.DEFAULT.dataset("fail"), DatasetProperties.EMPTY);
      }
    }, String.format(
      "Creating an instance of a type from %s should fail as %s does not have any privileges on it.", module1, BOB));

    // granting ADMIN on the module, BOB should now be able to create the dataset type
    grantAndAssertSuccess(module2, BOB, EnumSet.of(Action.ADMIN));
    grantAndAssertSuccess(type2, BOB, EnumSet.of(Action.ADMIN));

    // adding a module should now succeed as bob though, because bob has admin privilege on the module
    dsFramework.addModule(module2, new TestModule2(), createModuleJar(TestModule2.class));
    // get operation on module2 should succeed as Bob
    Assert.assertNotNull(dsFramework.getTypeInfo(type2));

    // but should fail as Alice since Alice does not have ADMIN on module2 or type2
    SecurityRequestContext.setUserId(ALICE.getName());
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

    // delete all instances so modules can be deleted
    SecurityRequestContext.setUserId(ALICE.getName());
    dsFramework.deleteAllInstances(NamespaceId.DEFAULT);
    SecurityRequestContext.setUserId(BOB.getName());
    // After granting admin on the modules, deleting all modules should succeed
    grantAndAssertSuccess(module1, BOB, EnumSet.of(Action.ADMIN));
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
