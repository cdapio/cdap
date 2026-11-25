/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.InstanceNotFoundException;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.InMemoryAccessControllerV2;
import io.cdap.cdap.security.authorization.RoleController;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link DatasetService} with authorization enabled
 */
public class DatasetServiceAuthorizationTest extends DatasetServiceTestBase {
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);

  private static RoleController roleController;
  private static PermissionManager permissionManager;

  @BeforeClass
  public static void setup() throws Exception {
    locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    initializeAndStartService(createCconf());
    roleController = injector.getInstance(RoleController.class);
    permissionManager = injector.getInstance(PermissionManager.class);
  }

  protected static CConfiguration createCconf() throws IOException {
    CConfiguration cConf = DatasetServiceTestBase.createCconf();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAccessControllerV2.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    return cConf;
  }

  @Test
  public void testDatasetInstances() throws Exception {
    final DatasetId dsId = NamespaceId.DEFAULT.dataset("myds");
    final DatasetId dsId1 = NamespaceId.DEFAULT.dataset("myds1");
    final DatasetId dsId2 = NamespaceId.DEFAULT.dataset("myds2");
    SecurityRequestContext.setUserId(ALICE.getName());
    assertAuthorizationFailure(
        () -> dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY),
        "Alice should not be able to add a dataset instance since she does not have ADMIN"
            + " privileges on the dataset");
    // grant alice full access to the dsId
    grantAndAssertSuccess(dsId, ALICE, EnumSet.allOf(StandardPermission.class));
    grantAndAssertSuccess(NamespaceId.DEFAULT, EntityType.DATASET, ALICE, EnumSet.of(StandardPermission.LIST));
    // now adding an instance should succeed
    dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY);
    // alice should be able to perform all operations on the dataset
    Assert.assertTrue(dsFramework.hasInstance(dsId));
    Assert.assertNotNull(dsFramework.getDataset(dsId, ImmutableMap.of(), null));
    dsFramework.updateInstance(dsId, DatasetProperties.builder().add("key", "value").build());
    // operations should fail for bob
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(() -> dsFramework.getDataset(dsId, ImmutableMap.of(), null),
                               String.format("Expected %s to not be have access to %s.", BOB, dsId));
    assertAuthorizationFailure(
      () -> dsFramework.updateInstance(dsId, DatasetProperties.builder().add("key", "val").build()),
      String.format("Expected %s to not be have %s privilege on %s.", BOB, StandardPermission.UPDATE, dsId));
    assertAuthorizationFailure(() -> dsFramework.truncateInstance(dsId),
                               String.format("Expected %s to not be have %s privilege on %s.",
                                             BOB, StandardPermission.UPDATE, dsId));
    grantAndAssertSuccess(dsId, BOB, ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE,
                                                     StandardPermission.DELETE));
    grantAndAssertSuccess(NamespaceId.DEFAULT, EntityType.DATASET, BOB, EnumSet.of(StandardPermission.LIST));
    // now update should succeed
    dsFramework.updateInstance(dsId, DatasetProperties.builder().add("key", "val").build());
    // as should truncate
    dsFramework.truncateInstance(dsId);
    DatasetSpecification datasetSpec = dsFramework.getDatasetSpec(dsId);
    Assert.assertNotNull(datasetSpec);
    Assert.assertEquals("val", datasetSpec.getProperty("key"));
    // grant Bob corresponding privilege to create the dataset
    grantAndAssertSuccess(dsId1, BOB, ImmutableSet.of(StandardPermission.CREATE));
    grantAndAssertSuccess(dsId2, BOB, ImmutableSet.of(StandardPermission.CREATE, StandardPermission.DELETE));
    dsFramework.addInstance(Table.class.getName(), dsId1, DatasetProperties.EMPTY);
    dsFramework.addInstance(Table.class.getName(), dsId2, DatasetProperties.EMPTY);
    // since Bob now has some privileges on all datasets, the list API should return all datasets for him
    Assert.assertEquals(ImmutableSet.of(dsId, dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    // Grant privileges on other datasets to user Alice
    grantAndAssertSuccess(dsId1, ALICE, ImmutableSet.of(ApplicationPermission.EXECUTE));
    grantAndAssertSuccess(dsId2, ALICE, ImmutableSet.of(ApplicationPermission.EXECUTE));

    SecurityRequestContext.setUserId(ALICE.getName());
    // Alice should not be able to delete any datasets since she does not have DELETE on all datasets in the namespace
    try {
      dsFramework.deleteAllInstances(NamespaceId.DEFAULT);
      Assert.fail();
    } catch (UnauthorizedException e) {
      //Expected
    }
    // alice should still be able to see all dataset instances
    Assert.assertEquals(ImmutableSet.of(dsId1, dsId2, dsId),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // should get an authorization error if alice tries to delete datasets that she does not have permissions on
    assertAuthorizationFailure(() -> dsFramework.deleteInstance(dsId1),
        String.format("Alice should not be able to delete instance %s since she does not "
            + "have privileges", dsId1));
    grantAndAssertSuccess(dsId1, ALICE,
        ImmutableSet.of(StandardPermission.DELETE, StandardPermission.CREATE));
    Assert.assertEquals(ImmutableSet.of(dsId1, dsId2, dsId),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    // since Alice now has DELETE for dsId1, she should be able to delete it
    dsFramework.deleteInstance(dsId1);

    // Now Alice only see dsId2 and dsId from list.
    Assert.assertEquals(ImmutableSet.of(dsId2, dsId),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));

    // Bob should be able to see dsId and dsId2
    SecurityRequestContext.setUserId(BOB.getName());
    Assert.assertEquals(ImmutableSet.of(dsId2, dsId),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    dsFramework.deleteInstance(dsId2);

    SecurityRequestContext.setUserId(ALICE.getName());
    dsFramework.deleteInstance(dsId);

    grantAndAssertSuccess(dsId2, ALICE, EnumSet.of(StandardPermission.CREATE, StandardPermission.DELETE));
    // add add the instance again
    dsFramework.addInstance(Table.class.getName(), dsId, DatasetProperties.EMPTY);
    dsFramework.addInstance(Table.class.getName(), dsId1, DatasetProperties.EMPTY);
    dsFramework.addInstance(Table.class.getName(), dsId2, DatasetProperties.EMPTY);

    Assert.assertEquals(ImmutableSet.of(dsId, dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT)));
    // should be successful since ALICE has DELETE on all datasets
    dsFramework.deleteAllInstances(NamespaceId.DEFAULT);
    Assert.assertTrue(dsFramework.getInstances(NamespaceId.DEFAULT).isEmpty());
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
    } catch (UnauthorizedException e) {
      //Expected
    }
    try {
      // user will not be able to check the existence on the instance since he does not have any privilege on the
      // instance
      dsFramework.hasInstance(nonExistingInstance);
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }
    SecurityRequestContext.setUserId(ALICE.getName());
    // user need to have access to the dataset to do any operations, even though the dataset does not exist
    grantAndAssertSuccess(nonExistingInstance, ALICE, EnumSet.allOf(StandardPermission.class));
    grantAndAssertSuccess(nonExistingModule, ALICE, EnumSet.allOf(StandardPermission.class));
    // after grant user should be able to check the dataset info
    Assert.assertNull(dsFramework.getDatasetSpec(nonExistingInstance));
    Assert.assertFalse(dsFramework.hasInstance(nonExistingInstance));
    assertNotFound(() -> dsFramework.updateInstance(nonExistingInstance, DatasetProperties.EMPTY),
                   String.format("Expected %s to not exist", nonExistingInstance));
    assertNotFound(() -> dsFramework.deleteInstance(nonExistingInstance),
                   String.format("Expected %s to not exist", nonExistingInstance));
    assertNotFound(() -> dsFramework.truncateInstance(nonExistingInstance),
                   String.format("Expected %s to not exist", nonExistingInstance));
    assertAuthorizationFailure(
      () -> dsFramework.addInstance(nonExistingType.getType(), nonExistingInstance, DatasetProperties.EMPTY),
      "Alice needs to have READ/ADMIN on the dataset type to create the dataset");
    assertNotFound(() -> dsFramework.deleteModule(nonExistingModule),
                   String.format("Expected %s to not exist", nonExistingModule));
    grantAndAssertSuccess(nonExistingType, ALICE, EnumSet.allOf(StandardPermission.class));
    Assert.assertNull(String.format("Expected %s to not exist", nonExistingType),
                      dsFramework.getTypeInfo(nonExistingType));
  }

  @Test
  public void testDatasetTypes() throws Exception {
    final DatasetModuleId module1 = NamespaceId.DEFAULT.datasetModule("existing1");
    final DatasetModuleId module2 = NamespaceId.DEFAULT.datasetModule("existing2");
    final DatasetTypeId type1 = NamespaceId.DEFAULT.datasetType("existingType1");
    final DatasetTypeId type2 = NamespaceId.DEFAULT.datasetType("existingType2");
    final DatasetId datasetId = NamespaceId.DEFAULT.dataset("fail");
    // write two modules to the store, as modules can only be listed and deleted through the dataset service
    writeModule(NamespaceId.DEFAULT,
                new DatasetModuleMeta(module1.getModule(), TestModule1x.class.getName(),
                                      createModuleJar(module1.getModule()).toURI(),
                                      ImmutableList.of(type1.getType()), Collections.emptyList()));
    writeModule(NamespaceId.DEFAULT,
                new DatasetModuleMeta(module2.getModule(), TestModule2.class.getName(),
                                      createModuleJar(module2.getModule()).toURI(),
                                      ImmutableList.of(type2.getType()), Collections.emptyList()));

    SecurityRequestContext.setUserId(ALICE.getName());
    assertAuthorizationFailure(
      () -> dsFramework.getTypeInfo(type1),
      String.format("Expected get type operation to fail for %s because she does not have %s on %s",
                    ALICE, StandardPermission.GET, type1));
    // grant alice GET on type1
    grantAndAssertSuccess(type1, ALICE, EnumSet.of(StandardPermission.GET));
    Assert.assertNotNull(dsFramework.getTypeInfo(type1));
    // but she still has no access to type2
    assertAuthorizationFailure(
      () -> dsFramework.getTypeInfo(type2),
      String.format("Expected get type operation to fail for %s because she does not have %s on %s",
                    ALICE, StandardPermission.GET, type2));

    // creating an instance of a type should fail as Bob, even if he can create the dataset
    SecurityRequestContext.setUserId(BOB.getName());
    grantAndAssertSuccess(datasetId, BOB, EnumSet.allOf(StandardPermission.class));
    assertAuthorizationFailure(
      () -> dsFramework.addInstance(type1.getType(), datasetId, DatasetProperties.EMPTY),
      String.format("Creating an instance of a type from %s should fail as %s does not have any privileges on it.",
                    module1, BOB));

    // deleting modules requires privileges on the modules
    SecurityRequestContext.setUserId(ALICE.getName());
    assertAuthorizationFailure(
      () -> dsFramework.deleteModule(module2),
      String.format("Deleting module %s should fail as %s does not have any privileges on it.", module2, ALICE));
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(
      () -> dsFramework.deleteModule(module1),
      String.format("Deleting module %s should fail as %s does not have any privileges on it.", module1, BOB));
    grantAndAssertSuccess(module1, BOB, EnumSet.of(StandardPermission.DELETE));
    assertAuthorizationFailure(
      () -> dsFramework.deleteAllModules(NamespaceId.DEFAULT),
      String.format("Deleting all modules in %s should fail as %s does not have DELETE privileges on %s.",
                    NamespaceId.DEFAULT, BOB, module2));

    // After granting DELETE on all the modules, deleting all modules should succeed
    grantAndAssertSuccess(module2, BOB, EnumSet.of(StandardPermission.DELETE));
    dsFramework.deleteAllModules(NamespaceId.DEFAULT);
    SecurityRequestContext.setUserId(ALICE.getName());
    Assert.assertNull(dsFramework.getTypeInfo(type1));
  }

  @Test
  public void testAddModuleNotSupported() throws Exception {
    final DatasetModuleId module = NamespaceId.DEFAULT.datasetModule("module3");
    SecurityRequestContext.setUserId(ALICE.getName());
    // adding a module is not supported, regardless of the privileges
    grantAndAssertSuccess(module, ALICE, EnumSet.allOf(StandardPermission.class));
    try {
      dsFramework.addModule(module, new TestModule1x());
      Assert.fail("Should not be able to add a dataset module");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    try {
      dsFramework.addModule(module, new TestModule1x(), createModuleJar(module.getModule()));
      Assert.fail("Should not be able to add a dataset module");
    } catch (UnsupportedOperationException e) {
      // expected
    }
    assertNotFound(() -> dsFramework.deleteModule(module), String.format("Expected %s to not exist", module));
  }

  @After
  public void cleanup() throws Exception {
    permissionManager.revoke(Authorizable.fromEntityId(NamespaceId.DEFAULT));
  }

  private Set<DatasetId> summaryToDatasetIdSet(Collection<DatasetSpecificationSummary> datasetSpecs) {
    Collection<DatasetId> datasetIds =
      Collections2.transform(datasetSpecs, input -> NamespaceId.DEFAULT.dataset(input.getName()));
    return ImmutableSet.copyOf(datasetIds);
  }

  private void grantAndAssertSuccess(EntityId entityId, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    grantAndAssertSuccess(entityId, null, principal, permissions);
  }

  private void grantAndAssertSuccess(EntityId entityId, EntityType childType,
                                     Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    Set<GrantedPermission> existingPrivileges = roleController.listGrants(principal);
    Authorizable authorizable = Authorizable.fromEntityId(entityId, childType);
    permissionManager.grant(authorizable, principal, permissions);
    ImmutableSet.Builder<GrantedPermission> expectedPrivilegesAfterGrant = ImmutableSet.builder();
    for (Permission permission : permissions) {
      expectedPrivilegesAfterGrant.add(new GrantedPermission(authorizable, permission));
    }
    Assert.assertEquals(Sets.union(existingPrivileges, expectedPrivilegesAfterGrant.build()),
                        roleController.listGrants(principal));
  }

  private void assertNotFound(DatasetOperationExecutor operation, String failureMsg) throws Exception {
    try {
      operation.execute();
      Assert.fail(failureMsg);
    } catch (InstanceNotFoundException expected) {
      // expected
    } catch (DatasetManagementException e) {
      // no other way to detect errors from DatasetServiceClient
      Assert.assertTrue("Wrong message: " + e.getMessage(),
                        e.getMessage().contains("Response code: 404, message: 'Not Found'"));
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
      Assert.assertTrue(e.getMessage().contains("Response code: 403, message: 'Forbidden'")
                          || e.getCause() instanceof UnauthorizedException);
    }
  }

  private interface DatasetOperationExecutor {
    void execute() throws Exception;
  }
}
