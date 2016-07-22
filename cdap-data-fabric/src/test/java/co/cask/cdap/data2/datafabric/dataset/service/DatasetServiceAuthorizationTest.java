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
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.DatasetId;
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
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

/**
 * Tests for {@link DatasetService} with authorization enabled
 */
public class DatasetServiceAuthorizationTest extends DatasetServiceTestBase {
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);

  private Authorizer authorizer;

  @Before
  public void setup() throws Exception {
    CConfiguration cConf = createCommonConf();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    initializeAndStartService(cConf);
    authorizer = injector.getInstance(AuthorizerInstantiator.class).get();
  }

  @Test
  public void testDatasetInstances() throws Exception {
    final DatasetId dsId = NamespaceId.DEFAULT.dataset("myds");
    final DatasetId dsId1 = NamespaceId.DEFAULT.dataset("myds1");
    DatasetId dsId2 = NamespaceId.DEFAULT.dataset("myds2");
    // grant alice write access to the namespace
    SecurityRequestContext.setUserId(ALICE.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.addInstance(Table.class.getName(), dsId.toId(), DatasetProperties.EMPTY);
      }
    }, "Alice should not be able to add a dataset instance since she does not have WRITE privileges on the namespace");
    grantAndAssertSuccess(NamespaceId.DEFAULT, ALICE, ImmutableSet.of(Action.WRITE));
    // now adding an instance should succeed
    addInstanceAndAssertPrivileges(ALICE, dsId);
    // alice should be able to perform all operations on the dataset
    Assert.assertTrue(dsFramework.hasInstance(dsId.toId()));
    Assert.assertNotNull(dsFramework.getDataset(dsId.toId(), ImmutableMap.<String, String>of(), null));
    dsFramework.updateInstance(dsId.toId(), DatasetProperties.builder().add("key", "value").build());
    // operations should fail for bob
    SecurityRequestContext.setUserId(BOB.getName());
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.getDataset(dsId.toId(), ImmutableMap.<String, String>of(), null);
      }
    }, String.format("Expected %s to not be have access to %s.", BOB, dsId));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.updateInstance(dsId.toId(), DatasetProperties.builder().add("key", "val").build());
      }
    }, String.format("Expected %s to not be have %s privilege on %s.", BOB, Action.ADMIN, dsId));
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.truncateInstance(dsId.toId());
      }
    }, String.format("Expected %s to not be have %s privilege on %s.", BOB, Action.ADMIN, dsId));
    grantAndAssertSuccess(dsId, BOB, ImmutableSet.of(Action.ADMIN));
    // now update should succeed
    dsFramework.updateInstance(dsId.toId(), DatasetProperties.builder().add("key", "val").build());
    // as should truncate
    dsFramework.truncateInstance(dsId.toId());
    DatasetSpecification datasetSpec = dsFramework.getDatasetSpec(dsId.toId());
    Assert.assertNotNull(datasetSpec);
    Assert.assertEquals("val", datasetSpec.getProperty("key"));
    // grant Bob WRITE on namespace, so he can add some datasets
    grantAndAssertSuccess(NamespaceId.DEFAULT, BOB, ImmutableSet.of(Action.WRITE));
    addInstanceAndAssertPrivileges(BOB, dsId1);
    addInstanceAndAssertPrivileges(BOB, dsId2);
    // since Bob now has some privileges on all datasets, the list API should return all datasets for him
    Assert.assertEquals(ImmutableSet.of(dsId, dsId1, dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT.toId())));
    // it should only return 1 dataset for Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    Assert.assertEquals(ImmutableSet.of(dsId),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT.toId())));
    dsFramework.deleteAllInstances(NamespaceId.DEFAULT.toId());
    // alice should now not see any instances
    Assert.assertTrue(dsFramework.getInstances(NamespaceId.DEFAULT.toId()).isEmpty());
    // should get an authorization error if alice tries to delete datasets that she does not have permissions on
    assertAuthorizationFailure(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteInstance(dsId1.toId());
      }
    }, String.format("Alice should not be able to delete instance %s since she does not have privileges", dsId1));
    grantAndAssertSuccess(dsId1, ALICE, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(ImmutableSet.of(dsId1),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT.toId())));
    // since Alice now is ADMIN for dsId1, she should be able to delete it
    deleteAndAssertPrivileges(dsId1);
    Assert.assertTrue(dsFramework.getInstances(NamespaceId.DEFAULT.toId()).isEmpty());
    // Bob should still see 1 instance
    SecurityRequestContext.setUserId(BOB.getName());
    Assert.assertEquals(ImmutableSet.of(dsId2),
                        summaryToDatasetIdSet(dsFramework.getInstances(NamespaceId.DEFAULT.toId())));
    deleteAndAssertPrivileges(dsId2);
  }

  @Test
  public void testNotFound() throws Exception {
    final Id.DatasetInstance doesnotexist = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "doesnotexist");
    Assert.assertNull(dsFramework.getDatasetSpec(doesnotexist));
    Assert.assertFalse(dsFramework.hasInstance(doesnotexist));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.updateInstance(doesnotexist, DatasetProperties.EMPTY);
      }
    }, String.format("Expected %s to not exist", doesnotexist));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.deleteInstance(doesnotexist);
      }
    }, String.format("Expected %s to not exist", doesnotexist));
    assertNotFound(new DatasetOperationExecutor() {
      @Override
      public void execute() throws Exception {
        dsFramework.truncateInstance(doesnotexist);
      }
    }, String.format("Expected %s to not exist", doesnotexist));
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
    dsFramework.addInstance(Table.class.getName(), dsId.toId(), DatasetProperties.EMPTY);
    Set<Privilege> after = authorizer.listPrivileges(principal);
    Assert.assertTrue(after.containsAll(before));
    Assert.assertEquals(ImmutableSet.of(new Privilege(dsId, Action.ALL)),
                        Sets.difference(after, before).immutableCopy());
  }

  private void deleteAndAssertPrivileges(final DatasetId dsId) throws Exception {
    dsFramework.deleteInstance(dsId.toId());
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
